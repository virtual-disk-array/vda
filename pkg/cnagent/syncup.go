package cnagent

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/virtual-disk-array/vda/pkg/lib"
	pbcn "github.com/virtual-disk-array/vda/pkg/proto/cnagentapi"
)

func newErrInfo(err error) *pbcn.ErrInfo {
	if err != nil {
		return &pbcn.ErrInfo{
			IsErr:     true,
			ErrMsg:    err.Error(),
			Timestamp: lib.ResTimestamp(),
		}
	} else {
		return &pbcn.ErrInfo{
			IsErr:     false,
			ErrMsg:    "",
			Timestamp: lib.ResTimestamp(),
		}
	}
}

type syncupHelper struct {
	lisConf      *lib.LisConf
	nf           *lib.NameFmt
	oc           *lib.OperationClient
	feNvmeMap    map[string]bool
	aggBdevMap   map[string]bool
	daLvsMap     map[string]bool
	snapMap      map[string]bool
	expNqnMap    map[string]bool
	grpBdevMap   map[string]bool
	raid0BdevMap map[string]bool
	secNvmeMap   map[string]bool
}

type bdevSeq struct {
	Name string
	Idx  uint32
}

func (sh *syncupHelper) syncupVdFe(cntlrFeReq *pbcn.CntlrFeReq,
	grpFeReq *pbcn.GrpFeReq, vdFeReq *pbcn.VdFeReq) *pbcn.VdFeRsp {
	var vdFeErr error
	beNqnName := sh.nf.BeNqnName(vdFeReq.VdId)
	feNvmeName := sh.nf.FeNvmeName(vdFeReq.VdId)
	sh.feNvmeMap[feNvmeName] = true
	feNqnName := sh.nf.FeNqnName(cntlrFeReq.CntlrId)
	lisConf := &lib.LisConf{
		TrType:  vdFeReq.VdFeConf.DnNvmfListener.TrType,
		AdrFam:  vdFeReq.VdFeConf.DnNvmfListener.AdrFam,
		TrAddr:  vdFeReq.VdFeConf.DnNvmfListener.TrAddr,
		TrSvcId: vdFeReq.VdFeConf.DnNvmfListener.TrSvcId,
	}

	if vdFeErr == nil {
		vdFeErr = sh.oc.CreateFeNvme(feNvmeName, beNqnName, feNqnName, lisConf)
	}

	vdFeRsp := &pbcn.VdFeRsp{
		VdId: vdFeReq.VdId,
		VdFeInfo: &pbcn.VdFeInfo{
			ErrInfo: newErrInfo(vdFeErr),
		},
	}
	return vdFeRsp
}

func (sh *syncupHelper) syncupGrpFe(cntlrFeReq *pbcn.CntlrFeReq,
	grpFeReq *pbcn.GrpFeReq) *pbcn.GrpFeRsp {
	var grpFeErr error
	vdFeRspList := make([]*pbcn.VdFeRsp, 0)
	vdFeReqList := grpFeReq.VdFeReqList
	sort.Slice(vdFeReqList, func(i, j int) bool {
		return vdFeReqList[i].VdFeConf.VdIdx > vdFeReqList[j].VdFeConf.VdIdx
	})
	feBdevList := make([]string, 0)
	for _, vdFeReq := range vdFeReqList {
		feBdevName := sh.nf.FeBdevName(vdFeReq.VdId)
		feBdevList = append(feBdevList, feBdevName)
		vdFeRsp := sh.syncupVdFe(cntlrFeReq, grpFeReq, vdFeReq)
		vdFeRspList = append(vdFeRspList, vdFeRsp)
		if vdFeRsp.VdFeInfo.ErrInfo.IsErr {
			grpFeErr = fmt.Errorf("VdFeError")
		}
	}

	raid0BdevName := sh.nf.Raid0BdevName(grpFeReq.GrpId)
	sh.raid0BdevMap[raid0BdevName] = true
	if grpFeErr == nil {
		grpFeErr = sh.oc.CreateRaid0Bdev(raid0BdevName,
			cntlrFeReq.CntlrFeConf.StripSizeKb, feBdevList)
	}

	grpBdevName := sh.nf.GrpBdevName(grpFeReq.GrpId)
	sh.grpBdevMap[grpBdevName] = true
	if grpFeErr == nil {
		grpFeErr = sh.oc.CreateGrpBdev(grpBdevName, raid0BdevName)
	}

	grpRsp := &pbcn.GrpFeRsp{
		GrpId: grpFeReq.GrpId,
		GrpFeInfo: &pbcn.GrpFeInfo{
			ErrInfo: newErrInfo(grpFeErr),
		},
		VdFeRspList: vdFeRspList,
	}
	return grpRsp
}

func (sh *syncupHelper) syncupSnapFe(cntlrFeReq *pbcn.CntlrFeReq,
	snapFeReq *pbcn.SnapFeReq) *pbcn.SnapFeRsp {
	var snapErr error
	daLvsName := sh.nf.DaLvsName(cntlrFeReq.CntlrFeConf.DaId)
	snapName := sh.nf.SnapName(snapFeReq.SnapId)
	oriName := sh.nf.SnapName(snapFeReq.SnapFeConf.OriId)
	isClone := snapFeReq.SnapFeConf.IsClone
	snapSize := snapFeReq.SnapFeConf.Size
	snapFullName := sh.nf.SnapFullName(cntlrFeReq.CntlrFeConf.DaId,
		snapFeReq.SnapId)
	sh.snapMap[snapFullName] = true
	if snapErr == nil {
		sh.oc.CreateSnap(daLvsName, snapName, oriName, isClone, snapSize)
	}
	snapFeRsp := &pbcn.SnapFeRsp{
		SnapId: snapFeReq.SnapId,
		SnapFeInfo: &pbcn.SnapFeInfo{
			ErrInfo: newErrInfo(snapErr),
		},
	}
	return snapFeRsp
}

func (sh *syncupHelper) syncupExpFe(cntlrFeReq *pbcn.CntlrFeReq,
	expFeReq *pbcn.ExpFeReq, secNqnList []string) *pbcn.ExpFeRsp {
	var expFeErr error
	expNqnName := sh.nf.ExpNqnName(expFeReq.ExpId)
	sh.expNqnMap[expNqnName] = true
	snapFullName := sh.nf.SnapFullName(cntlrFeReq.CntlrFeConf.DaId, expFeReq.ExpFeConf.SnapId)
	initiatorNqn := expFeReq.ExpFeConf.InitiatorNqn
	lisConf := sh.lisConf

	if expFeErr == nil {
		expFeErr = sh.oc.EnableHistogram(snapFullName)
	}

	if expFeErr == nil {
		expFeErr = sh.oc.CreateExpPrimaryNvmf(expNqnName,
			snapFullName, initiatorNqn, secNqnList, lisConf)
	}

	expFeRsp := &pbcn.ExpFeRsp{
		ExpId: expFeReq.ExpId,
		ExpFeInfo: &pbcn.ExpFeInfo{
			ErrInfo: newErrInfo(expFeErr),
		},
	}
	return expFeRsp
}

func (sh *syncupHelper) syncupPrimary(cntlrFeReq *pbcn.CntlrFeReq,
	secNqnList []string) *pbcn.CntlrFeRsp {
	var cntlrFeErr error
	grpFeRspList := make([]*pbcn.GrpFeRsp, 0)
	snapFeRspList := make([]*pbcn.SnapFeRsp, 0)
	expFeRspList := make([]*pbcn.ExpFeRsp, 0)

	grpFeReqList := cntlrFeReq.GrpFeReqList
	sort.Slice(grpFeReqList, func(i, j int) bool {
		return grpFeReqList[i].GrpFeConf.GrpIdx > grpFeReqList[j].GrpFeConf.GrpIdx
	})
	grpBdevList := make([]string, 0)
	for _, grpFeReq := range grpFeReqList {
		grpBdevName := sh.nf.GrpBdevName(grpFeReq.GrpId)
		grpBdevList = append(grpBdevList, grpBdevName)
		grpFeRsp := sh.syncupGrpFe(cntlrFeReq, grpFeReq)
		grpFeRspList = append(grpFeRspList, grpFeRsp)
		if grpFeRsp.GrpFeInfo.ErrInfo.IsErr {
			cntlrFeErr = fmt.Errorf("GrpFeError")
		}
	}

	aggBdevName := sh.nf.AggBdevName(cntlrFeReq.CntlrFeConf.DaId)
	sh.aggBdevMap[aggBdevName] = true
	if cntlrFeErr == nil {
		cntlrFeErr = sh.oc.CreateAggBdev(aggBdevName, grpBdevList)
	}
	if cntlrFeErr == nil {
		cntlrFeErr = sh.oc.ExamineBdev(aggBdevName)
	}
	daLvsName := sh.nf.DaLvsName(cntlrFeReq.CntlrFeConf.DaId)
	sh.daLvsMap[daLvsName] = true
	if cntlrFeErr == nil {
		cntlrFeErr = sh.oc.CreateDaLvs(daLvsName, aggBdevName)
	}

	if cntlrFeErr == nil {
		snapFeReqList := cntlrFeReq.SnapFeReqList
		sort.Slice(snapFeReqList, func(i, j int) bool {
			return snapFeReqList[i].SnapFeConf.Idx > snapFeReqList[j].SnapFeConf.Idx
		})
		for _, snapFeReq := range snapFeReqList {
			snapFeRsp := sh.syncupSnapFe(cntlrFeReq, snapFeReq)
			snapFeRspList = append(snapFeRspList, snapFeRsp)
		}
	}

	if cntlrFeErr == nil {
		for _, expFeReq := range cntlrFeReq.ExpFeReqList {
			expFeRsp := sh.syncupExpFe(cntlrFeReq, expFeReq, secNqnList)
			expFeRspList = append(expFeRspList, expFeRsp)
		}
	}

	cntlrFeRsp := &pbcn.CntlrFeRsp{
		CntlrId: cntlrFeReq.CntlrId,
		CntlrFeInfo: &pbcn.CntlrFeInfo{
			ErrInfo: newErrInfo(cntlrFeErr),
		},
		GrpFeRspList:  grpFeRspList,
		SnapFeRspList: snapFeRspList,
		ExpFeRspList:  expFeRspList,
	}
	return cntlrFeRsp
}

func (sh *syncupHelper) syncupSecExpFe(cntlrFeReq *pbcn.CntlrFeReq,
	expFeReq *pbcn.ExpFeReq, primCntlr *pbcn.Controller) *pbcn.ExpFeRsp {
	var expFeErr error
	secNvmeName := sh.nf.SecNvmeName(primCntlr.CntlrId)
	sh.secNvmeMap[secNvmeName] = true
	secBdevName := sh.nf.SecBdevName(primCntlr.CntlrId)
	expNqnName := sh.nf.ExpNqnName(expFeReq.ExpId)
	sh.expNqnMap[expNqnName] = true
	secNqnName := sh.nf.SecNqnName(cntlrFeReq.CntlrId)
	primLisConf := &lib.LisConf{
		TrType:  primCntlr.CnNvmfListener.TrType,
		AdrFam:  primCntlr.CnNvmfListener.AdrFam,
		TrAddr:  primCntlr.CnNvmfListener.TrAddr,
		TrSvcId: primCntlr.CnNvmfListener.TrSvcId,
	}

	if expFeErr == nil {
		expFeErr = sh.oc.CreateSecNvme(secNvmeName,
			expNqnName, secNqnName, primLisConf)
	}

	if expFeErr == nil {
		expFeErr = sh.oc.EnableHistogram(secBdevName)
	}

	if expFeErr == nil {
		expFeErr = sh.oc.CreateExpSecNvmf(expNqnName,
			secBdevName, expFeReq.ExpFeConf.InitiatorNqn, sh.lisConf)
	}

	expFeRsp := &pbcn.ExpFeRsp{
		ExpId: expFeReq.ExpId,
		ExpFeInfo: &pbcn.ExpFeInfo{
			ErrInfo: newErrInfo(expFeErr),
		},
	}
	return expFeRsp

}

func (sh *syncupHelper) syncupSecondary(cntlrFeReq *pbcn.CntlrFeReq,
	primCntlr *pbcn.Controller) *pbcn.CntlrFeRsp {
	var cntlrFeErr error
	expFeRspList := make([]*pbcn.ExpFeRsp, 0)

	if cntlrFeErr == nil {
		for _, expFeReq := range cntlrFeReq.ExpFeReqList {
			expFeRsp := sh.syncupSecExpFe(cntlrFeReq, expFeReq, primCntlr)
			expFeRspList = append(expFeRspList, expFeRsp)
		}
	}

	cntlrFeRsp := &pbcn.CntlrFeRsp{
		CntlrId: cntlrFeReq.CntlrId,
		CntlrFeInfo: &pbcn.CntlrFeInfo{
			ErrInfo: newErrInfo(cntlrFeErr),
		},
		GrpFeRspList:  make([]*pbcn.GrpFeRsp, 0),
		SnapFeRspList: make([]*pbcn.SnapFeRsp, 0),
		ExpFeRspList:  expFeRspList,
	}
	return cntlrFeRsp
}

func (sh *syncupHelper) syncupCntlrFe(cntlrFeReq *pbcn.CntlrFeReq) *pbcn.CntlrFeRsp {
	var cntlrFeErr error
	var thisCntlr *pbcn.Controller
	var primCntlr *pbcn.Controller
	secNqnList := make([]string, 0)
	for _, cntlr := range cntlrFeReq.CntlrFeConf.CntlrList {
		if cntlrFeReq.CntlrId == cntlr.CntlrId {
			thisCntlr = cntlr
		}
		if cntlr.IsPrimary {
			primCntlr = cntlr
		} else {
			secNqnName := sh.nf.SecNqnName(cntlr.CntlrId)
			secNqnList = append(secNqnList, secNqnName)
		}
	}

	if thisCntlr == nil {
		cntlrFeErr = fmt.Errorf("can not find thisCntlr")
	}
	if primCntlr == nil {
		cntlrFeErr = fmt.Errorf("can not find primCntlr")
	}

	if cntlrFeErr == nil {
		if thisCntlr.IsPrimary {
			return sh.syncupPrimary(cntlrFeReq, secNqnList)
		} else {
			return sh.syncupSecondary(cntlrFeReq, primCntlr)
		}
	} else {
		return &pbcn.CntlrFeRsp{
			CntlrId: cntlrFeReq.CntlrId,
			CntlrFeInfo: &pbcn.CntlrFeInfo{
				ErrInfo: newErrInfo(cntlrFeErr),
			},
			GrpFeRspList:  make([]*pbcn.GrpFeRsp, 0),
			SnapFeRspList: make([]*pbcn.SnapFeRsp, 0),
			ExpFeRspList:  make([]*pbcn.ExpFeRsp, 0),
		}
	}
}

func (sh *syncupHelper) syncupCn(cnReq *pbcn.CnReq) *pbcn.CnRsp {
	var cnErr error
	cntlrFeRspList := make([]*pbcn.CntlrFeRsp, 0)
	cnErr = sh.oc.LoadNvmfs()

	if cnErr == nil {
		for _, cntlrFeReq := range cnReq.CntlrFeReqList {
			cntlrFeRsp := sh.syncupCntlrFe(cntlrFeReq)
			cntlrFeRspList = append(cntlrFeRspList, cntlrFeRsp)
		}
	}

	if cnErr == nil {
		cnErr = sh.oc.LoadBdevs()
	}

	if cnErr == nil {
		expNqnPrefix := sh.nf.ExpNqnPrefix()
		expNqnList, cnErr := sh.oc.GetExpNqnList(expNqnPrefix)
		if cnErr == nil {
			for _, expNqnName := range expNqnList {
				_, ok := sh.expNqnMap[expNqnName]
				if !ok {
					cnErr = sh.oc.DeleteExpNvmf(expNqnName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		secNvmfPrefix := sh.nf.SecNvmePrefix()
		secNvmeList, cnErr := sh.oc.GetSecNvmeList(secNvmfPrefix)
		if cnErr == nil {
			for _, secNvmeName := range secNvmeList {
				_, ok := sh.secNvmeMap[secNvmeName]
				if !ok {
					cnErr = sh.oc.DeleteSecNvme(secNvmeName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		snapFullNamePrefix := sh.nf.SnapFullNamePrefix()
		snapFullNameList, cnErr := sh.oc.GetSnapList(snapFullNamePrefix)
		if cnErr == nil {
			for _, snapFullName := range snapFullNameList {
				_, ok := sh.snapMap[snapFullName]
				if !ok {
					cnErr = sh.oc.DeleteSnap(snapFullName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		daLvsPrefix := sh.nf.DaLvsPrefix()
		daLvsList, cnErr := sh.oc.GetDaLvsList(daLvsPrefix)
		if cnErr == nil {
			for _, daLvsName := range daLvsList {
				_, ok := sh.daLvsMap[daLvsName]
				if !ok {
					cnErr = sh.oc.DeleteDaLvs(daLvsName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		aggBdevPrefix := sh.nf.AggBdevPrefix()
		aggBdevList, cnErr := sh.oc.GetAggBdevList(aggBdevPrefix)
		if cnErr == nil {
			for _, aggBdevName := range aggBdevList {
				_, ok := sh.aggBdevMap[aggBdevName]
				if !ok {
					cnErr = sh.oc.DeleteAggBdev(aggBdevName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		grpBdevPrefix := sh.nf.GrpBdevPrefix()
		grpBdevList, cnErr := sh.oc.GetGrpBdevList(grpBdevPrefix)
		if cnErr == nil {
			for _, grpBdevName := range grpBdevList {
				_, ok := sh.grpBdevMap[grpBdevName]
				if !ok {
					cnErr = sh.oc.DeleteGrpBdev(grpBdevName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		raid0BdevPrefix := sh.nf.Raid0BdevPrefix()
		raid0BdevList, cnErr := sh.oc.GetRaid0BdevList(raid0BdevPrefix)
		if cnErr == nil {
			for _, raid0BdevName := range raid0BdevList {
				_, ok := sh.raid0BdevMap[raid0BdevName]
				if !ok {
					cnErr = sh.oc.DeleteRaid0Bdev(raid0BdevName)
					if cnErr != nil {
						break
					}
				}
			}
		}
	}

	if cnErr == nil {
		feNvmePrefix := sh.nf.FeNvmePrefix()
		feNvmeList, cnErr := sh.oc.GetFeNvmeList(feNvmePrefix)
		if cnErr == nil {
			for _, feNvmeName := range feNvmeList {
				cnErr = sh.oc.DeleteFeNvme(feNvmeName)
				if cnErr != nil {
					break
				}
			}
		}
	}

	return &pbcn.CnRsp{
		CnId: cnReq.CnId,
		CnInfo: &pbcn.CnInfo{
			ErrInfo: newErrInfo(cnErr),
		},
		CntlrFeRspList: cntlrFeRspList,
	}
}

func newSyncupHelper(lisConf *lib.LisConf, nf *lib.NameFmt, sc *lib.SpdkClient) *syncupHelper {
	return &syncupHelper{
		lisConf:      lisConf,
		nf:           nf,
		oc:           lib.NewOperationClient(sc),
		feNvmeMap:    make(map[string]bool),
		aggBdevMap:   make(map[string]bool),
		daLvsMap:     make(map[string]bool),
		snapMap:      make(map[string]bool),
		expNqnMap:    make(map[string]bool),
		secNvmeMap:   make(map[string]bool),
		grpBdevMap:   make(map[string]bool),
		raid0BdevMap: make(map[string]bool),
	}
}

func (cnAgent *cnAgentServer) SyncupCn(ctx context.Context, req *pbcn.SyncupCnRequest) (
	*pbcn.SyncupCnReply, error) {
	cnMutex.Lock()
	defer cnMutex.Unlock()
	currVersion := atomic.LoadUint64(&lastVersion)
	if req.Version < currVersion {
		return &pbcn.SyncupCnReply{
			ReplyInfo: &pbcn.ReplyInfo{
				ReplyCode: lib.CnOldRevErrCode,
				ReplyMsg: fmt.Sprintf("received rev: %d, current rev: %d",
					req.Version, currVersion),
			},
		}, nil
	}
	atomic.StoreUint64(&lastVersion, req.Version)
	sh := newSyncupHelper(cnAgent.lisConf, cnAgent.nf, cnAgent.sc)

	cnRsp := sh.syncupCn(req.CnReq)
	return &pbcn.SyncupCnReply{
		ReplyInfo: &pbcn.ReplyInfo{
			ReplyCode: lib.CnSucceedCode,
			ReplyMsg:  lib.CnSucceedMsg,
		},
		CnRsp: cnRsp,
	}, nil
}
