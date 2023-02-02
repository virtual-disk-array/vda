package lib

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/protobuf/proto"
	"google.golang.org/grpc"

	"github.com/virtual-disk-array/vda/pkg/logger"
	pbcn "github.com/virtual-disk-array/vda/pkg/proto/cnagentapi"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
	pbdn "github.com/virtual-disk-array/vda/pkg/proto/dnagentapi"
)

type SyncupManager struct {
	kf *KeyFmt
	sw *StmWrapper
}

type dnIdToRes struct {
	idToDn   map[string]*pbdn.DnRsp
	idToPd   map[string]*pbdn.PdRsp
	idToVdBe map[string]*pbdn.VdBeRsp
}

type cnIdToRes struct {
	idToCn      map[string]*pbcn.CnRsp
	idToCntlrFe map[string]*pbcn.CntlrFeRsp
	idToGrpFe   map[string]*pbcn.GrpFeRsp
	idToVdFe    map[string]*pbcn.VdFeRsp
	idToSnapFe  map[string]*pbcn.SnapFeRsp
	idToExpFe   map[string]*pbcn.ExpFeRsp
	idToMtFe    map[string]*pbcn.MtFeRsp
}

type capDiff struct {
	old string
	new string
	val string
}

func setDnErrInfo(from *pbdn.ErrInfo, to *pbds.ErrInfo) bool {
	if from == nil {
		to.IsErr = true
		to.ErrMsg = ResNoInfoMsg
		to.Timestamp = ResTimestamp()
	} else {
		to.IsErr = from.IsErr
		to.ErrMsg = from.ErrMsg
		to.Timestamp = from.Timestamp
	}
	return to.IsErr
}

func setCnErrInfo(from *pbcn.ErrInfo, to *pbds.ErrInfo) bool {
	if from == nil {
		to.IsErr = true
		to.ErrMsg = ResNoInfoMsg
		to.Timestamp = ResTimestamp()
	} else {
		to.IsErr = from.IsErr
		to.ErrMsg = from.ErrMsg
		to.Timestamp = from.Timestamp
	}
	return to.IsErr
}

func (sm *SyncupManager) getDiskNode(sockAddr string, ctx context.Context) (
	int64, *pbds.DiskNode, error) {
	var revision int64
	diskNode := &pbds.DiskNode{}
	dnEntityKey := sm.kf.DnEntityKey(sockAddr)

	apply := func(stm concurrency.STM) error {
		dnEntityVal := []byte(stm.Get(dnEntityKey))
		if len(dnEntityVal) == 0 {
			logger.Warning("Can not find diskNode %s", sockAddr)
			return fmt.Errorf("Can not find diskNode %s", sockAddr)
		}
		err := proto.Unmarshal(dnEntityVal, diskNode)
		if err != nil {
			logger.Error("Unmarshal diskNode err %s %v", sockAddr, err)
			return err
		}
		revision = stm.Rev(dnEntityKey)
		logger.Debug("SyncupManager diskNode get: %v revision: %v",
			diskNode, revision)
		return nil
	}

	err := sm.sw.RunStm(apply, ctx, "SyncupDnGet: "+sockAddr)
	return revision, diskNode, err
}

func (sm *SyncupManager) buildSyncupDnRequest(
	diskNode *pbds.DiskNode, ctx context.Context) (
	*pbdn.SyncupDnRequest, error) {

	pdReqList := make([]*pbdn.PdReq, 0)
	for _, physicalDisk := range diskNode.PdList {
		pdId := physicalDisk.PdId
		pdConf := &pbdn.PdConf{}
		switch x := physicalDisk.PdConf.BdevType.(type) {
		case *pbds.PdConf_BdevMalloc:
			pdConf.BdevType = &pbdn.PdConf_BdevMalloc{
				BdevMalloc: &pbdn.BdevMalloc{
					Size: x.BdevMalloc.Size,
				},
			}
		case *pbds.PdConf_BdevAio:
			pdConf.BdevType = &pbdn.PdConf_BdevAio{
				BdevAio: &pbdn.BdevAio{
					FileName: x.BdevAio.FileName,
				},
			}
		case *pbds.PdConf_BdevNvme:
			pdConf.BdevType = &pbdn.PdConf_BdevNvme{
				BdevNvme: &pbdn.BdevNvme{
					TrAddr: x.BdevNvme.TrAddr,
				},
			}
		case nil:
			logger.Error("BdevType is empty: %v", diskNode)
			return nil, fmt.Errorf("BdevType is empty")
		default:
			logger.Error("Unknown BdevType: %v", diskNode)
			return nil, fmt.Errorf("unknown BdevType")
		}
		vdBeReqList := []*pbdn.VdBeReq{}
		for _, vdBackend := range physicalDisk.VdBeList {
			vdId := vdBackend.VdId
			vdBeConf := &pbdn.VdBeConf{
				Size: vdBackend.VdBeConf.Size,
				Qos: &pbdn.BdevQos{
					RwIosPerSec:    vdBackend.VdBeConf.Qos.RwIosPerSec,
					RwMbytesPerSec: vdBackend.VdBeConf.Qos.RwMbytesPerSec,
					RMbytesPerSec:  vdBackend.VdBeConf.Qos.RMbytesPerSec,
					WMbytesPerSec:  vdBackend.VdBeConf.Qos.WMbytesPerSec,
				},
				CntlrId: vdBackend.VdBeConf.CntlrId,
			}
			vdBeReq := &pbdn.VdBeReq{
				VdId:     vdId,
				VdBeConf: vdBeConf,
			}
			vdBeReqList = append(vdBeReqList, vdBeReq)
		}
		pdReq := &pbdn.PdReq{
			PdId:        pdId,
			PdConf:      pdConf,
			VdBeReqList: vdBeReqList,
		}
		pdReqList = append(pdReqList, pdReq)
	}
	dnId := diskNode.DnId
	dnReq := &pbdn.DnReq{
		DnId:      dnId,
		PdReqList: pdReqList,
	}
	req := &pbdn.SyncupDnRequest{
		ReqId:   GetReqId(ctx),
		Version: diskNode.Version,
		DnReq:   dnReq,
	}
	return req, nil
}

func (sm *SyncupManager) getDnRsp(reply *pbdn.SyncupDnReply, idToRes *dnIdToRes) {
	if reply.DnRsp == nil {
		return
	}
	dnRsp := reply.DnRsp
	idToRes.idToDn[dnRsp.DnId] = dnRsp
	if dnRsp.PdRspList != nil {
		for _, pdRsp := range dnRsp.PdRspList {
			idToRes.idToPd[pdRsp.PdId] = pdRsp
			if pdRsp.VdBeRspList != nil {
				for _, vdBeRsp := range pdRsp.VdBeRspList {
					idToRes.idToVdBe[vdBeRsp.VdId] = vdBeRsp
				}
			}
		}
	}
}

func (sm *SyncupManager) setDnInfo(diskNode *pbds.DiskNode, idToRes *dnIdToRes) ([]*capDiff, bool) {
	capDiffList := make([]*capDiff, 0)
	var isErr bool
	dnRsp, ok := idToRes.idToDn[diskNode.DnId]
	if ok {
		if setDnErrInfo(dnRsp.DnInfo.ErrInfo, diskNode.DnInfo.ErrInfo) {
			isErr = true
		}
	} else {
		if setDnErrInfo(nil, diskNode.DnInfo.ErrInfo) {
			isErr = true
		}
	}

	for _, physicalDisk := range diskNode.PdList {
		pdRsp, ok := idToRes.idToPd[physicalDisk.PdId]
		oldFreeSize := physicalDisk.Capacity.FreeSize
		if ok {
			if setDnErrInfo(pdRsp.PdInfo.ErrInfo, physicalDisk.PdInfo.ErrInfo) {
				isErr = true
			}
			physicalDisk.Capacity.TotalSize = pdRsp.PdCapacity.TotalSize
			physicalDisk.Capacity.FreeSize = pdRsp.PdCapacity.FreeSize
		} else {
			if setDnErrInfo(nil, physicalDisk.PdInfo.ErrInfo) {
				isErr = true
			}
			physicalDisk.Capacity.TotalSize = 0
			physicalDisk.Capacity.FreeSize = 0
		}
		newFreeSize := physicalDisk.Capacity.FreeSize
		if oldFreeSize != newFreeSize {
			dnSearchAttr := &pbds.DnSearchAttr{
				PdCapacity: &pbds.PdCapacity{
					TotalSize: physicalDisk.Capacity.TotalSize,
					FreeSize:  physicalDisk.Capacity.FreeSize,
					TotalQos: &pbds.BdevQos{
						RwIosPerSec:    physicalDisk.Capacity.TotalQos.RwIosPerSec,
						RwMbytesPerSec: physicalDisk.Capacity.TotalQos.RwMbytesPerSec,
						RMbytesPerSec:  physicalDisk.Capacity.TotalQos.RMbytesPerSec,
						WMbytesPerSec:  physicalDisk.Capacity.TotalQos.WMbytesPerSec,
					},
					FreeQos: &pbds.BdevQos{
						RwIosPerSec:    physicalDisk.Capacity.FreeQos.RwIosPerSec,
						RwMbytesPerSec: physicalDisk.Capacity.FreeQos.RwMbytesPerSec,
						RMbytesPerSec:  physicalDisk.Capacity.FreeQos.RMbytesPerSec,
						WMbytesPerSec:  physicalDisk.Capacity.FreeQos.WMbytesPerSec,
					},
				},
			}
			val, err := proto.Marshal(dnSearchAttr)
			if err != nil {
				logger.Error("Can not marshal dnSearchAttr: %v", dnSearchAttr)
			} else {
				cd := &capDiff{
					old: sm.kf.DnCapKey(oldFreeSize, diskNode.SockAddr, physicalDisk.PdName),
					new: sm.kf.DnCapKey(newFreeSize, diskNode.SockAddr, physicalDisk.PdName),
					val: string(val),
				}
				capDiffList = append(capDiffList, cd)
			}
		}
		for _, vdBackend := range physicalDisk.VdBeList {
			vdBeRsp, ok := idToRes.idToVdBe[vdBackend.VdId]
			if ok {
				if setDnErrInfo(vdBeRsp.VdBeInfo.ErrInfo, vdBackend.VdBeInfo.ErrInfo) {
					isErr = true
				}
			} else {
				if setDnErrInfo(nil, vdBackend.VdBeInfo.ErrInfo) {
					isErr = true
				}
			}
		}
	}
	return capDiffList, isErr
}

func (sm *SyncupManager) writeDnInfo(diskNode *pbds.DiskNode, capDiffList []*capDiff,
	isErr bool, revision int64, ctx context.Context) {

	logger.Debug("SyncupManager diskNode put: %v", diskNode)
	dnEntityKey := sm.kf.DnEntityKey(diskNode.SockAddr)
	dnEntityVal, err := proto.Marshal(diskNode)
	if err != nil {
		logger.Error("Marshal diskNode err: %v %v", diskNode, err)
		return
	}
	dnEntityValStr := string(dnEntityVal)

	dnErrKey := sm.kf.DnErrKey(diskNode.DnConf.HashCode, diskNode.SockAddr)
	dnSummary := &pbds.DnSummary{
		Description: diskNode.DnConf.Description,
	}
	dnErrVal, err := proto.Marshal(dnSummary)
	if err != nil {
		logger.Error("Marshal dnSummary err: %v %v", dnSummary, err)
		return
	}
	dnErrValStr := string(dnErrVal)

	apply := func(stm concurrency.STM) error {
		rev1 := stm.Rev(dnEntityKey)
		if rev1 != revision {
			logger.Warning("revision does not match, give up, old: %v new: %v",
				revision, rev1)
			return nil
		}
		stm.Put(dnEntityKey, dnEntityValStr)
		if isErr {
			stm.Put(dnErrKey, dnErrValStr)
		} else {
			stm.Del(dnErrKey)
		}
		for _, cd := range capDiffList {
			logger.Debug("Dn capacity change 1, old: %s new: %s",
				cd.old, cd.new)
			stm.Del(cd.old)
			stm.Put(cd.new, cd.val)
		}
		return nil
	}

	err = sm.sw.RunStm(apply, ctx, "SyncupDnPut: "+diskNode.SockAddr)
	if err != nil {
		logger.Error("RunStm err: %v", err)
	}
}

func (sm *SyncupManager) syncupDn(sockAddr string, ctx context.Context,
	req *pbdn.SyncupDnRequest) (*pbdn.SyncupDnReply, error) {
	conn, err := grpc.Dial(sockAddr, grpc.WithInsecure())
	if err != nil {
		logger.Warning("Get conn err: %s %v", sockAddr, err)
		return nil, err
	}
	defer conn.Close()
	c := pbdn.NewDnAgentClient(conn)
	logger.Info("SyncupDn req: %s %v", sockAddr, req)
	reply, err := c.SyncupDn(ctx, req)
	if err != nil {
		logger.Warning("SyncupDn err: %v", err)
	} else {
		logger.Info("SyncupDn reply: %v", reply)
	}
	return reply, err
}

func (sm *SyncupManager) SyncupDn(sockAddr string, ctx context.Context) {
	revision, diskNode, err := sm.getDiskNode(sockAddr, ctx)
	if err != nil {
		return
	}
	req, err := sm.buildSyncupDnRequest(diskNode, ctx)
	if err != nil {
		return
	}

	idToRes := &dnIdToRes{
		idToDn:   make(map[string]*pbdn.DnRsp),
		idToPd:   make(map[string]*pbdn.PdRsp),
		idToVdBe: make(map[string]*pbdn.VdBeRsp),
	}

	reply, err := sm.syncupDn(sockAddr, ctx, req)
	logger.Info("%v %v %v", reply, err, idToRes)
	if err == nil {
		if reply.ReplyInfo.ReplyCode == DnSucceedCode {
			sm.getDnRsp(reply, idToRes)
			capDiffList, isErr := sm.setDnInfo(diskNode, idToRes)
			sm.writeDnInfo(diskNode, capDiffList, isErr, revision, ctx)
		} else {
			logger.Warning("SyncupDn reply error: %v", reply.ReplyInfo)
		}
	} else {
		logger.Warning("SyncupDn grpc error: %v", err)
		capDiffList, isErr := sm.setDnInfo(diskNode, idToRes)
		sm.writeDnInfo(diskNode, capDiffList, isErr, revision, ctx)
	}
}

func (sm *SyncupManager) getControllerNode(sockAddr string, ctx context.Context) (
	int64, *pbds.ControllerNode, error) {
	var revision int64
	controllerNode := &pbds.ControllerNode{}
	cnEntityKey := sm.kf.CnEntityKey(sockAddr)

	apply := func(stm concurrency.STM) error {
		cnEntityVal := []byte(stm.Get(cnEntityKey))
		if len(cnEntityVal) == 0 {
			logger.Warning("Can not find controllerNode %s", sockAddr)
			return fmt.Errorf("Can not find controllerNode %s", sockAddr)
		}
		err := proto.Unmarshal(cnEntityVal, controllerNode)
		if err != nil {
			logger.Error("Unmarshal1 controllerNode err %s %v", sockAddr, err)
			return err
		}
		revision = stm.Rev(cnEntityKey)
		logger.Debug("SyncupManager controllerNode get: %v revision: %v",
			controllerNode, revision)
		return nil
	}

	err := sm.sw.RunStm(apply, ctx, "SyncupCnGet: "+sockAddr)
	return revision, controllerNode, err
}

func (sm *SyncupManager) buildSyncupCnRequest(
	controllerNode *pbds.ControllerNode, ctx context.Context) (
	*pbcn.SyncupCnRequest, error) {

	cntlrFeReqList := make([]*pbcn.CntlrFeReq, 0)
	for _, cntlrFe := range controllerNode.CntlrFeList {
		grpFeReqList := make([]*pbcn.GrpFeReq, 0)
		for _, grpFe := range cntlrFe.GrpFeList {
			vdFeReqList := make([]*pbcn.VdFeReq, 0)
			for _, vdFe := range grpFe.VdFeList {
				vdFeReq := &pbcn.VdFeReq{
					VdId: vdFe.VdId,
					VdFeConf: &pbcn.VdFeConf{
						DnNvmfListener: &pbcn.NvmfListener{
							TrType:  vdFe.VdFeConf.DnNvmfListener.TrType,
							AdrFam:  vdFe.VdFeConf.DnNvmfListener.AdrFam,
							TrAddr:  vdFe.VdFeConf.DnNvmfListener.TrAddr,
							TrSvcId: vdFe.VdFeConf.DnNvmfListener.TrSvcId,
						},
						DnSockAddr: vdFe.VdFeConf.DnSockAddr,
						VdIdx:      vdFe.VdFeConf.VdIdx,
						Size:       vdFe.VdFeConf.Size,
					},
				}
				vdFeReqList = append(vdFeReqList, vdFeReq)
			}
			grpFeReq := &pbcn.GrpFeReq{
				GrpId: grpFe.GrpId,
				GrpFeConf: &pbcn.GrpFeConf{
					GrpIdx: grpFe.GrpFeConf.GrpIdx,
					Size:   grpFe.GrpFeConf.Size,
				},
				VdFeReqList: vdFeReqList,
			}
			grpFeReqList = append(grpFeReqList, grpFeReq)
		}
		snapFeReqList := make([]*pbcn.SnapFeReq, 0)
		for _, snapFe := range cntlrFe.SnapFeList {
			snapFeReq := &pbcn.SnapFeReq{
				SnapId: snapFe.SnapId,
				SnapFeConf: &pbcn.SnapFeConf{
					OriId:   snapFe.SnapFeConf.OriId,
					Idx:     snapFe.SnapFeConf.Idx,
					Size:    snapFe.SnapFeConf.Size,
				},
			}
			snapFeReqList = append(snapFeReqList, snapFeReq)
		}
		expFeReqList := make([]*pbcn.ExpFeReq, 0)
		for _, expFe := range cntlrFe.ExpFeList {
			expFeReq := &pbcn.ExpFeReq{
				ExpId: expFe.ExpId,
				ExpFeConf: &pbcn.ExpFeConf{
					InitiatorNqn: expFe.ExpFeConf.InitiatorNqn,
					SnapId:       expFe.ExpFeConf.SnapId,
					DaName:       expFe.ExpFeConf.DaName,
					ExpName:      expFe.ExpFeConf.ExpName,
				},
			}
			expFeReqList = append(expFeReqList, expFeReq)
		}
		mtFeReqList := make([]*pbcn.MtFeReq, 0)
		for _, mtFe := range cntlrFe.MtFeList {
			mtFeReq := &pbcn.MtFeReq{
				MtId: mtFe.MtId,
				MtFeConf: &pbcn.MtFeConf{
					GrpIdx: mtFe.MtFeConf.GrpIdx,
					VdIdx: mtFe.MtFeConf.VdIdx,
					SrcListener: &pbcn.NvmfListener{
						TrType:  mtFe.MtFeConf.SrcListener.TrType,
						AdrFam:  mtFe.MtFeConf.SrcListener.AdrFam,
						TrAddr:  mtFe.MtFeConf.SrcListener.TrAddr,
						TrSvcId: mtFe.MtFeConf.SrcListener.TrSvcId,
					},
					SrcVdId: mtFe.MtFeConf.SrcVdId,
					DstListener: &pbcn.NvmfListener{
						TrType:  mtFe.MtFeConf.DstListener.TrType,
						AdrFam:  mtFe.MtFeConf.DstListener.AdrFam,
						TrAddr:  mtFe.MtFeConf.DstListener.TrAddr,
						TrSvcId: mtFe.MtFeConf.DstListener.TrSvcId,
					},
					DstVdId: mtFe.MtFeConf.DstVdId,
					Raid1Conf: &pbcn.Raid1Conf{
						BitSizeKb: mtFe.MtFeConf.Raid1Conf.BitSizeKb,
					},
				},
			}
			mtFeReqList = append(mtFeReqList, mtFeReq)
		}
		cntlrList := make([]*pbcn.Controller, 0)
		for _, cntlr := range cntlrFe.CntlrFeConf.CntlrList {
			cntlr1 := &pbcn.Controller{
				CntlrId:    cntlr.CntlrId,
				CnSockAddr: cntlr.CnSockAddr,
				CntlrIdx:   cntlr.CntlrIdx,
				IsPrimary:  cntlr.IsPrimary,
				CnNvmfListener: &pbcn.NvmfListener{
					TrType:  cntlr.CnNvmfListener.TrType,
					AdrFam:  cntlr.CnNvmfListener.AdrFam,
					TrAddr:  cntlr.CnNvmfListener.TrAddr,
					TrSvcId: cntlr.CnNvmfListener.TrSvcId,
				},
			}
			cntlrList = append(cntlrList, cntlr1)
		}
		cntlrFeReq := &pbcn.CntlrFeReq{
			CntlrId: cntlrFe.CntlrId,
			CntlrFeConf: &pbcn.CntlrFeConf{
				DaId:        cntlrFe.CntlrFeConf.DaId,
				CntlrList:   cntlrList,
				Size:        cntlrFe.CntlrFeConf.Size,
				LvsConf:     &pbcn.LvsConf{
					ClusterSize: cntlrFe.CntlrFeConf.LvsConf.ClusterSize,
					ExtendRatio: cntlrFe.CntlrFeConf.LvsConf.ExtendRatio,
				},
				Raid0Conf: &pbcn.Raid0Conf{
					StripSizeKb: cntlrFe.CntlrFeConf.Raid0Conf.StripSizeKb,
					BdevCnt: cntlrFe.CntlrFeConf.Raid0Conf.BdevCnt,
				},
			},
			IsInited:      cntlrFe.IsInited,
			GrpFeReqList:  grpFeReqList,
			SnapFeReqList: snapFeReqList,
			ExpFeReqList:  expFeReqList,
			MtFeReqList:   mtFeReqList,
		}
		if cntlrFe.CntlrFeConf.Redundancy != nil {
			switch x := cntlrFe.CntlrFeConf.Redundancy.(type) {
			case *pbds.CntlrFeConf_RedunRaid1Conf:
				cntlrFeReq.CntlrFeConf.Redundancy = &pbcn.CntlrFeConf_RedunRaid1Conf{
					RedunRaid1Conf: &pbcn.RedunRaid1Conf{
						Raid1Conf: &pbcn.Raid1Conf{
							BitSizeKb: x.RedunRaid1Conf.Raid1Conf.BitSizeKb,
						},
						SingleHealthy: x.RedunRaid1Conf.SingleHealthy,
					},
				}
			default:
				logger.Warning("Unknow redundancy: %v", x)
			}
		}
		cntlrFeReqList = append(cntlrFeReqList, cntlrFeReq)
	}
	cnReq := &pbcn.CnReq{
		CnId:           controllerNode.CnId,
		CntlrFeReqList: cntlrFeReqList,
	}
	req := &pbcn.SyncupCnRequest{
		ReqId:   GetReqId(ctx),
		Version: controllerNode.Version,
		CnReq:   cnReq,
	}
	return req, nil
}

func (sm *SyncupManager) syncupCn(sockAddr string, ctx context.Context,
	req *pbcn.SyncupCnRequest) (*pbcn.SyncupCnReply, error) {
	conn, err := grpc.Dial(sockAddr, grpc.WithInsecure())
	if err != nil {
		logger.Warning("Get conn err: %s %v", sockAddr, err)
		return nil, err
	}
	defer conn.Close()
	c := pbcn.NewCnAgentClient(conn)
	logger.Info("SyncupCn req: %s %v", sockAddr, req)
	reply, err := c.SyncupCn(ctx, req)
	if err != nil {
		logger.Warning("SyncupCn err: %v", err)
	} else {
		logger.Info("SyncupCn reply: %v", reply)
	}
	return reply, err
}

func (sm *SyncupManager) getCnRsp(reply *pbcn.SyncupCnReply, idToRes *cnIdToRes) {
	if reply.CnRsp == nil {
		return
	}
	cnRsp := reply.CnRsp
	idToRes.idToCn[cnRsp.CnId] = cnRsp
	if cnRsp.CntlrFeRspList != nil {
		for _, cntlrFeRsp := range cnRsp.CntlrFeRspList {
			idToRes.idToCntlrFe[cntlrFeRsp.CntlrId] = cntlrFeRsp
			if cntlrFeRsp.GrpFeRspList != nil {
				for _, grpFeRsp := range cntlrFeRsp.GrpFeRspList {
					idToRes.idToGrpFe[grpFeRsp.GrpId] = grpFeRsp
					if grpFeRsp.VdFeRspList != nil {
						for _, vdFeRsp := range grpFeRsp.VdFeRspList {
							idToRes.idToVdFe[vdFeRsp.VdId] = vdFeRsp
						}
					}
				}
			}
			if cntlrFeRsp.SnapFeRspList != nil {
				for _, snapFeRsp := range cntlrFeRsp.SnapFeRspList {
					idToRes.idToSnapFe[snapFeRsp.SnapId] = snapFeRsp
				}
			}
			if cntlrFeRsp.ExpFeRspList != nil {
				for _, expFeRsp := range cntlrFeRsp.ExpFeRspList {
					idToRes.idToExpFe[expFeRsp.ExpId] = expFeRsp
				}
			}
			if cntlrFeRsp.MtFeRspList != nil {
				for _, mtFeRsp := range cntlrFeRsp.MtFeRspList {
					idToRes.idToMtFe[mtFeRsp.MtId] = mtFeRsp
				}
			}
		}
	}
}

func (sm *SyncupManager) setCnInfo(controllerNode *pbds.ControllerNode,
	idToRes *cnIdToRes) ([]*capDiff, bool) {
	var isErr bool
	capDiffList := make([]*capDiff, 0)
	cnRsp, ok := idToRes.idToCn[controllerNode.CnId]
	if ok {
		if setCnErrInfo(cnRsp.CnInfo.ErrInfo, controllerNode.CnInfo.ErrInfo) {
			isErr = true
		}
	} else {
		if setCnErrInfo(nil, controllerNode.CnInfo.ErrInfo) {
			isErr = true
		}
	}

	for _, cntlrFe := range controllerNode.CntlrFeList {
		cntlrFeRsp, ok := idToRes.idToCntlrFe[cntlrFe.CntlrId]
		if ok {
			if setCnErrInfo(cntlrFeRsp.CntlrFeInfo.ErrInfo, cntlrFe.CntlrFeInfo.ErrInfo) {
				isErr = true
			}
		} else {
			if setCnErrInfo(nil, cntlrFe.CntlrFeInfo.ErrInfo) {
				isErr = true
			}
		}
		var thisCntlr *pbds.Controller
		for _, cntlr := range cntlrFe.CntlrFeConf.CntlrList {
			if cntlr.CntlrId == cntlrFe.CntlrId {
				thisCntlr = cntlr
				break
			}
		}
		if thisCntlr == nil {
			logger.Error("Can not find thisCntlr: %s %v", controllerNode.SockAddr, cntlrFe)
		} else {
			if thisCntlr.IsPrimary && !cntlrFe.CntlrFeInfo.ErrInfo.IsErr {
				cntlrFe.IsInited = true
			}
		}
		for _, grpFe := range cntlrFe.GrpFeList {
			grpFeRsp, ok := idToRes.idToGrpFe[grpFe.GrpId]
			if ok {
				if setCnErrInfo(grpFeRsp.GrpFeInfo.ErrInfo, grpFe.GrpFeInfo.ErrInfo) {
					isErr = true
				}
			} else {
				if setCnErrInfo(nil, grpFe.GrpFeInfo.ErrInfo) {
					isErr = true
				}
			}
			for _, vdFe := range grpFe.VdFeList {
				vdFeRsp, ok := idToRes.idToVdFe[vdFe.VdId]
				if ok {
					if setCnErrInfo(vdFeRsp.VdFeInfo.ErrInfo, vdFe.VdFeInfo.ErrInfo) {
						isErr = true
					}
				} else {
					if setCnErrInfo(nil, vdFe.VdFeInfo.ErrInfo) {
						isErr = true
					}
				}
			}
		}
		for _, snapFe := range cntlrFe.SnapFeList {
			snapFeRsp, ok := idToRes.idToSnapFe[snapFe.SnapId]
			if ok {
				if setCnErrInfo(snapFeRsp.SnapFeInfo.ErrInfo,
					snapFe.SnapFeInfo.ErrInfo) {
					isErr = true
				}
			} else {
				if setCnErrInfo(nil, snapFe.SnapFeInfo.ErrInfo) {
					isErr = true
				}
			}
		}
		for _, expFe := range cntlrFe.ExpFeList {
			expFeRsp, ok := idToRes.idToExpFe[expFe.ExpId]
			if ok {
				if setCnErrInfo(expFeRsp.ExpFeInfo.ErrInfo,
					expFe.ExpFeInfo.ErrInfo) {
					isErr = true
				}
			} else {
				if setCnErrInfo(nil, expFe.ExpFeInfo.ErrInfo) {
					isErr = true
				}
			}
		}
		for _, mtFe := range cntlrFe.MtFeList {
			mtFeRsp, ok := idToRes.idToMtFe[mtFe.MtId]
			if  ok {
				if setCnErrInfo(mtFeRsp.MtFeInfo.ErrInfo,
					mtFe.MtFeInfo.ErrInfo) {
					isErr = true
				}
				mtFe.MtFeInfo.Raid1Info = &pbds.Raid1Info{
					Bdev0Online: mtFeRsp.MtFeInfo.Raid1Info.Bdev0Online,
					Bdev1Online: mtFeRsp.MtFeInfo.Raid1Info.Bdev1Online,
					TotalBit: mtFeRsp.MtFeInfo.Raid1Info.TotalBit,
					SyncedBit: mtFeRsp.MtFeInfo.Raid1Info.SyncedBit,
					ResyncIoCnt: mtFeRsp.MtFeInfo.Raid1Info.ResyncIoCnt,
					Status: mtFeRsp.MtFeInfo.Raid1Info.Status,
				}
			} else {
				if setCnErrInfo(nil, mtFe.MtFeInfo.ErrInfo) {
					isErr = true
				}
			}
		}
	}
	return capDiffList, isErr
}

func (sm *SyncupManager) writeCnInfo(controllerNode *pbds.ControllerNode,
	capDiffList []*capDiff, isErr bool, revision int64, ctx context.Context) {

	logger.Debug("SyncupManager controllerNode put: %v", controllerNode)
	cnEntityKey := sm.kf.CnEntityKey(controllerNode.SockAddr)
	cnEntityVal, err := proto.Marshal(controllerNode)
	if err != nil {
		logger.Error("Marshal controllerNode err: %v %v", controllerNode, err)
		return
	}
	cnEntityValStr := string(cnEntityVal)

	cnErrKey := sm.kf.CnErrKey(controllerNode.CnConf.HashCode, controllerNode.SockAddr)
	cnSummary := &pbds.CnSummary{
		Description: controllerNode.CnConf.Description,
	}
	cnErrVal, err := proto.Marshal(cnSummary)
	if err != nil {
		logger.Error("Marshal cnSummary err: %v %v", cnSummary, err)
		return
	}
	cnErrValStr := string(cnErrVal)

	apply := func(stm concurrency.STM) error {
		rev1 := stm.Rev(cnEntityKey)
		if rev1 != revision {
			logger.Warning("revision does not match, give up, old: %v new: %v",
				revision, rev1)
			return nil
		}
		stm.Put(cnEntityKey, cnEntityValStr)
		if isErr {
			stm.Put(cnErrKey, cnErrValStr)
		} else {
			stm.Del(cnErrKey)
		}
		for _, cd := range capDiffList {
			stm.Del(cd.old)
			stm.Put(cd.new, cd.val)
		}
		return nil
	}

	err = sm.sw.RunStm(apply, ctx, "SyncupCnPut: "+controllerNode.SockAddr)
	if err != nil {
		logger.Error("RunStm err: %v", err)
	}
}

func (sm *SyncupManager) SyncupCn(sockAddr string, ctx context.Context) {
	revision, controllerNode, err := sm.getControllerNode(sockAddr, ctx)
	if err != nil {
		return
	}
	req, err := sm.buildSyncupCnRequest(controllerNode, ctx)
	if err != nil {
		return
	}

	idToRes := &cnIdToRes{
		idToCn:      make(map[string]*pbcn.CnRsp),
		idToCntlrFe: make(map[string]*pbcn.CntlrFeRsp),
		idToGrpFe:   make(map[string]*pbcn.GrpFeRsp),
		idToVdFe:    make(map[string]*pbcn.VdFeRsp),
		idToSnapFe:  make(map[string]*pbcn.SnapFeRsp),
		idToExpFe:   make(map[string]*pbcn.ExpFeRsp),
		idToMtFe:    make(map[string]*pbcn.MtFeRsp),
	}

	reply, err := sm.syncupCn(sockAddr, ctx, req)
	if err == nil {
		if reply.ReplyInfo.ReplyCode == CnSucceedCode {
			sm.getCnRsp(reply, idToRes)
			capDiffList, isErr := sm.setCnInfo(controllerNode, idToRes)
			sm.writeCnInfo(controllerNode, capDiffList, isErr, revision, ctx)
		} else {
			logger.Warning("SyncupCn reply error: %v", reply.ReplyInfo)
		}
	} else {
		logger.Warning("SyncupCn grpc error: %v", err)
		capDiffList, isErr := sm.setCnInfo(controllerNode, idToRes)
		sm.writeCnInfo(controllerNode, capDiffList, isErr, revision, ctx)
	}
}

func NewSyncupManager(kf *KeyFmt, sw *StmWrapper) *SyncupManager {
	return &SyncupManager{
		kf: kf,
		sw: sw,
	}
}
