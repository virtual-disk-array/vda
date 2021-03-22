package dnagent

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbdn "github.com/virtual-disk-array/vda/pkg/proto/dnagentapi"
)

func newErrInfo(err error) *pbdn.ErrInfo {
	if err != nil {
		return &pbdn.ErrInfo{
			IsErr:     true,
			ErrMsg:    err.Error(),
			Timestamp: lib.ResTimestamp(),
		}
	} else {
		return &pbdn.ErrInfo{
			IsErr:     false,
			ErrMsg:    "",
			Timestamp: lib.ResTimestamp(),
		}
	}
}

type syncupHelper struct {
	lisConf   *lib.LisConf
	nf        *lib.NameFmt
	oc        *lib.OperationClient
	pdBdevMap map[string]bool
	pdLvsMap  map[string]bool
	beLvolMap map[string]bool
	beNqnMap  map[string]bool
}

func (sh *syncupHelper) syncupVdBe(vdBeReq *pbdn.VdBeReq,
	pdId string, pdLvsName string) *pbdn.VdBeRsp {
	var vdBeErr error
	vdBeErr = nil
	beLvolName := sh.nf.BeLvolName(vdBeReq.VdId)
	beLvolFullName := sh.nf.BeLvolFullName(pdId, vdBeReq.VdId)
	sh.beLvolMap[beLvolFullName] = true
	if vdBeErr == nil {
		vdBeErr = sh.oc.CreateBeLvol(pdLvsName, beLvolName, vdBeReq.VdBeConf.Size,
			vdBeReq.VdBeConf.Qos.RwIosPerSec, vdBeReq.VdBeConf.Qos.RwMbytesPerSec,
			vdBeReq.VdBeConf.Qos.RMbytesPerSec, vdBeReq.VdBeConf.Qos.WMbytesPerSec)
	}
	beNqnName := sh.nf.BeNqnName(vdBeReq.VdId)
	sh.beNqnMap[beNqnName] = true
	feNqnName := sh.nf.FeNqnName(vdBeReq.VdBeConf.CntlrId)
	if vdBeErr == nil {
		vdBeErr = sh.oc.CreateBeNvmf(beNqnName, beLvolFullName, feNqnName, sh.lisConf)
	}
	vdBeRsp := &pbdn.VdBeRsp{
		VdId: vdBeReq.VdId,
		VdBeInfo: &pbdn.VdBeInfo{
			ErrInfo: newErrInfo(vdBeErr),
		},
	}
	return vdBeRsp
}

func (sh *syncupHelper) syncupPd(pdReq *pbdn.PdReq) *pbdn.PdRsp {
	var pdErr error
	vdBeRspList := make([]*pbdn.VdBeRsp, 0)
	var realName string

	pdBdevName := sh.nf.PdBdevName(pdReq.PdId)
	switch x := pdReq.PdConf.BdevType.(type) {
	case *pbdn.PdConf_BdevMalloc:
		realName = pdBdevName
		pdErr = sh.oc.CreatePdMalloc(pdBdevName, x.BdevMalloc.Size)
	case *pbdn.PdConf_BdevAio:
		realName = pdBdevName
		pdErr = sh.oc.CreatePdAio(pdBdevName, x.BdevAio.FileName)
	case *pbdn.PdConf_BdevNvme:
		realName = pdBdevName + "n1"
		pdErr = sh.oc.CreatePdNvme(pdBdevName, realName, x.BdevNvme.TrAddr)
	case nil:
		logger.Error("BdevType is empty, %v", *pdReq)
		pdErr = fmt.Errorf("BdevType empty")
	default:
		logger.Error("Unknow BdevType, %v", *pdReq)
		pdErr = fmt.Errorf("Unknon BdevType")
	}
	sh.pdBdevMap[realName] = true

	if pdErr == nil {
		pdErr = sh.oc.ExamineBdev(realName)
	}
	if pdErr == nil {
		pdErr = sh.oc.EnableHistogram(realName)
	}
	pdLvsName := sh.nf.PdLvsName(pdReq.PdId)
	sh.pdLvsMap[pdLvsName] = true
	if pdErr == nil {
		pdErr = sh.oc.CreatePdLvs(pdLvsName, realName)
	}
	if pdErr == nil {
		for _, vdBeReq := range pdReq.VdBeReqList {
			vdBeRsp := sh.syncupVdBe(vdBeReq, pdReq.PdId, pdLvsName)
			vdBeRspList = append(vdBeRspList, vdBeRsp)
		}
	}

	totalSize := uint64(0)
	freeSize := uint64(0)
	if pdErr == nil {
		lvsInfo, pdErr := sh.oc.GetLvsInfo(pdLvsName)
		if pdErr == nil {
			totalSize = lvsInfo.TotalDataClusters * lvsInfo.ClusterSize
			freeSize = lvsInfo.FreeClusters * lvsInfo.ClusterSize
		}
	}

	return &pbdn.PdRsp{
		PdId: pdReq.PdId,
		PdInfo: &pbdn.PdInfo{
			ErrInfo: newErrInfo(pdErr),
		},
		PdCapacity: &pbdn.PdCapacity{
			TotalSize: totalSize,
			FreeSize:  freeSize,
		},
		VdBeRspList: vdBeRspList,
	}
}

func (sh *syncupHelper) syncupDn(dnReq *pbdn.DnReq) *pbdn.DnRsp {
	var dnErr error
	pdRspList := make([]*pbdn.PdRsp, 0)
	dnErr = sh.oc.LoadNvmfs()
	if dnErr == nil {
		for _, pdReq := range dnReq.PdReqList {
			pdRsp := sh.syncupPd(pdReq)
			pdRspList = append(pdRspList, pdRsp)
		}
	}

	if dnErr == nil {
		dnErr = sh.oc.LoadBdevs()
	}

	if dnErr == nil {
		beNqnPrefix := sh.nf.BeNqnPrefix()
		beNqnList, dnErr := sh.oc.GetBeNqnList(beNqnPrefix)
		if dnErr == nil {
			for _, nqnName := range beNqnList {
				_, ok := sh.beNqnMap[nqnName]
				if !ok {
					dnErr = sh.oc.DeleteBeNvmf(nqnName)
					if dnErr != nil {
						break
					}
				}
			}
		}
	}

	if dnErr == nil {
		beLvolPrefix := sh.nf.BeLvolPrefix()
		beLvolList, dnErr := sh.oc.GetBeLvolList(beLvolPrefix)
		if dnErr == nil {
			for _, fullName := range beLvolList {
				_, ok := sh.beLvolMap[fullName]
				if !ok {
					dnErr = sh.oc.DeleteBeLvol(fullName)
					if dnErr != nil {
						break
					}
				}
			}
		}
	}

	if dnErr == nil {
		pdLvsPrefix := sh.nf.PdLvsPrefix()
		pdLvsList, dnErr := sh.oc.GetPdLvsList(pdLvsPrefix)
		if dnErr == nil {
			for _, lvsName := range pdLvsList {
				_, ok := sh.pdLvsMap[lvsName]
				if !ok {
					dnErr = sh.oc.DeletePdLvs(lvsName)
					if dnErr != nil {
						break
					}
				}
			}
		}
	}

	if dnErr == nil {
		pdBdevPrefix := sh.nf.PdBdevPrefix()
		pdBdevList, dnErr := sh.oc.GetPdBdevList(pdBdevPrefix)
		if dnErr == nil {
			for _, bdevName := range pdBdevList {
				_, ok := sh.pdBdevMap[bdevName]
				if !ok {
					dnErr = sh.oc.DeletePdBdev(bdevName)
					if dnErr != nil {
						break
					}
				}
			}
		}
	}

	return &pbdn.DnRsp{
		DnId: dnReq.DnId,
		DnInfo: &pbdn.DnInfo{
			ErrInfo: newErrInfo(dnErr),
		},
		PdRspList: pdRspList,
	}
}

func newSyncupHelper(lisConf *lib.LisConf, nf *lib.NameFmt, sc *lib.SpdkClient) *syncupHelper {
	return &syncupHelper{
		lisConf:   lisConf,
		nf:        nf,
		oc:        lib.NewOperationClient(sc),
		pdBdevMap: make(map[string]bool),
		pdLvsMap:  make(map[string]bool),
		beNqnMap:  make(map[string]bool),
		beLvolMap: make(map[string]bool),
	}
}

func (dnAgent *dnAgentServer) SyncupDn(ctx context.Context, req *pbdn.SyncupDnRequest) (
	*pbdn.SyncupDnReply, error) {
	dnMutex.Lock()
	defer dnMutex.Unlock()
	logger.Debug("SyncupDn get lock: %v", req)
	currVersion := atomic.LoadUint64(&lastVersion)
	if req.Version < currVersion {
		return &pbdn.SyncupDnReply{
			ReplyInfo: &pbdn.ReplyInfo{
				ReplyCode: lib.DnOldRevErrCode,
				ReplyMsg: fmt.Sprintf("received rev: %d, current rev: %d",
					req.Version, currVersion),
			},
		}, nil
	}
	atomic.StoreUint64(&lastVersion, req.Version)
	sh := newSyncupHelper(dnAgent.lisConf, dnAgent.nf, dnAgent.sc)

	dnRsp := sh.syncupDn(req.DnReq)
	return &pbdn.SyncupDnReply{
		ReplyInfo: &pbdn.ReplyInfo{
			ReplyCode: lib.DnSucceedCode,
			ReplyMsg:  lib.DnSucceedMsg,
		},
		DnRsp: dnRsp,
	}, nil
}
