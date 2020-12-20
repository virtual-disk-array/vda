package lib

import (
	"context"
	"fmt"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"

	"github.com/virtual-disk-array/vda/pkg/logger"
	pbcn "github.com/virtual-disk-array/vda/pkg/proto/cnagentapi"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
	pbdn "github.com/virtual-disk-array/vda/pkg/proto/dnagentapi"
)

type SyncupManager struct {
	kf         *KeyFmt
	sw         *StmWrapper
	createConn func(sockAddr string) (*grpc.ClientConn, error)
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
}

type capDiff struct {
	old string
	new string
	val string
}

func (sm *SyncupManager) SyncupDn(sockAddr string, ctx context.Context) {
	revision, diskNode, err := sm.getDiskNode(sockAddr, ctx)
	if err != nil {
		return
	}
	req, err := sm.buildSyncupDnRequest(revision, diskNode, ctx)
	if err != nil {
		return
	}

	idToRes := &dnIdToRes{
		idToDn:   make(map[string]*pbdn.DnRsp),
		idToPd:   make(map[string]*pbdn.PdRsp),
		idToVdBe: make(map[string]*pbdn.VdBeRsp),
	}

	reply, err := sm.syncupDn(sockAddr, ctx, req)
	if err == nil {
		if reply.ReplyInfo.ReplyCode == DnSucceedCode {
			sm.getDnRsp(reply, idToRes)
			capDiffList := sm.setDnInfo(diskNode, idToRes)
			sm.writeDnInfo(diskNode, capDiffList, revision, ctx)
		} else {
			logger.Warning("SyncupDn reply error: %v", reply.ReplyInfo)
		}
	} else {
		logger.Warning("SyncupDn grpc error: %v", err)
		capDiffList := sm.setDnInfo(diskNode, idToRes)
		sm.writeDnInfo(diskNode, capDiffList, revision, ctx)
	}
}

func (sm *SyncupManager) getDiskNode(sockAddr string, ctx context.Context) (
	int64, *pbds.DiskNode, error) {
	var revision int64
	diskNode := &pbds.DiskNode{}
	dnEntityKey := sm.kf.DnEntityKey(sockAddr)

	apply := func(stm concurrency.STM) error {
		dnEntityVal := []byte(stm.Get(dnEntityKey))
		if len(dnEntityVal) == 0 {
			logger.Warning("can not find diskNode %s", sockAddr)
			return fmt.Errorf("can not find diskNode")
		}
		err := proto.Unmarshal(dnEntityVal, diskNode)
		if err != nil {
			logger.Error("unmarshal diskNode failed %s %v", sockAddr, err)
			return err
		}
		revision = stm.Rev(dnEntityKey)
		return nil
	}

	err := sm.sw.RunStm(apply, ctx, "SyncupDnGet: "+sockAddr)
	return revision, diskNode, err
}

func (sm *SyncupManager) buildSyncupDnRequest(
	revision int64, diskNode *pbds.DiskNode, ctx context.Context) (
	*pbdn.SyncupDnRequest, error) {

	pdReqList := []*pbdn.PdReq{}
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
			logger.Error("BdevType is empty, %v", diskNode)
			return nil, fmt.Errorf("BdevType is empty")
		default:
			logger.Error("unknown BdevType, %v", diskNode)
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
		ReqId:    GetReqId(ctx),
		Revision: revision,
		DnReq:    dnReq,
	}
	return req, nil
}

func (sm *SyncupManager) getDnRsp(reply *pbdn.SyncupDnReply, idToRes *dnIdToRes) {
	dnRsp := reply.DnRsp
	idToRes.idToDn[dnRsp.DnId] = dnRsp
	for _, pdRsp := range dnRsp.PdRspList {
		idToRes.idToPd[pdRsp.PdId] = pdRsp
		for _, vdBeRsp := range pdRsp.VdBeRspList {
			idToRes.idToVdBe[vdBeRsp.VdId] = vdBeRsp
		}
	}
}

func (sm *SyncupManager) setDnInfo(diskNode *pbds.DiskNode, idToRes *dnIdToRes) []*capDiff {
	capDiffList := make([]*capDiff, 0)
	dnRsp, ok := idToRes.idToDn[diskNode.DnId]
	if ok {
		diskNode.DnInfo.ErrInfo.IsErr = dnRsp.DnInfo.ErrInfo.IsErr
		diskNode.DnInfo.ErrInfo.ErrMsg = dnRsp.DnInfo.ErrInfo.ErrMsg
		diskNode.DnInfo.ErrInfo.Timestamp = dnRsp.DnInfo.ErrInfo.Timestamp
	} else {
		diskNode.DnInfo.ErrInfo.IsErr = true
		diskNode.DnInfo.ErrInfo.ErrMsg = ResNoInfoMsg
		diskNode.DnInfo.ErrInfo.Timestamp = ResTimestamp()
	}

	for _, physicalDisk := range diskNode.PdList {
		pdRsp, ok := idToRes.idToPd[physicalDisk.PdId]
		oldFreeSize := physicalDisk.Capacity.FreeSize
		if ok {
			physicalDisk.PdInfo.ErrInfo.IsErr = pdRsp.PdInfo.ErrInfo.IsErr
			physicalDisk.PdInfo.ErrInfo.ErrMsg = pdRsp.PdInfo.ErrInfo.ErrMsg
			physicalDisk.PdInfo.ErrInfo.Timestamp = pdRsp.PdInfo.ErrInfo.Timestamp
			physicalDisk.Capacity.TotalSize = pdRsp.PdCapacity.TotalSize
			physicalDisk.Capacity.FreeSize = pdRsp.PdCapacity.FreeSize
		} else {
			physicalDisk.PdInfo.ErrInfo.IsErr = true
			physicalDisk.PdInfo.ErrInfo.ErrMsg = ResNoInfoMsg
			physicalDisk.PdInfo.ErrInfo.Timestamp = ResTimestamp()
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
				vdBackend.VdBeInfo.ErrInfo.IsErr = vdBeRsp.VdBeInfo.ErrInfo.IsErr
				vdBackend.VdBeInfo.ErrInfo.ErrMsg = vdBeRsp.VdBeInfo.ErrInfo.ErrMsg
				vdBackend.VdBeInfo.ErrInfo.Timestamp = vdBeRsp.VdBeInfo.ErrInfo.Timestamp
			} else {
				vdBackend.VdBeInfo.ErrInfo.IsErr = true
				vdBackend.VdBeInfo.ErrInfo.ErrMsg = ResNoInfoMsg
				vdBackend.VdBeInfo.ErrInfo.Timestamp = ResTimestamp()
			}
		}
	}
	return capDiffList
}

func (sm *SyncupManager) writeDnInfo(diskNode *pbds.DiskNode, capDiffList []*capDiff,
	revision int64, ctx context.Context) {

	dnEntityKey := sm.kf.DnEntityKey(diskNode.SockAddr)
	dnEntityVal, err := proto.Marshal(diskNode)
	if err != nil {
		logger.Error("Marshal diskNode failed: %v %v", diskNode, err)
		return
	}
	dnEntityValStr := string(dnEntityVal)

	dnErrKey := sm.kf.DnErrKey(diskNode.DnConf.HashCode, diskNode.SockAddr)
	dnSummary := &pbds.DnSummary{
		Description: diskNode.DnConf.Description,
	}
	dnErrVal, err := proto.Marshal(dnSummary)
	if err != nil {
		logger.Error("Marshal dnSummary failed: %v %v", dnSummary, err)
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
		if diskNode.DnInfo.ErrInfo.IsErr {
			stm.Put(dnErrKey, dnErrValStr)
		} else {
			stm.Del(dnErrKey)
		}
		for _, cd := range capDiffList {
			stm.Del(cd.old)
			stm.Put(cd.new, cd.val)
		}
		return nil
	}

	err = sm.sw.RunStm(apply, ctx, "SyncupDnPut: "+diskNode.SockAddr)
	if err != nil {
		logger.Error("RunStm failed: %v", err)
	}
}

func (sm *SyncupManager) syncupDn(sockAddr string, ctx context.Context,
	req *pbdn.SyncupDnRequest) (*pbdn.SyncupDnReply, error) {
	conn, err := sm.createConn(sockAddr)
	if err != nil {
		logger.Warning("Create conn failed: %s %v", sockAddr, err)
		return nil, err
	}
	c := pbdn.NewDnAgentClient(conn)
	logger.Info("SyncupDn req: %s %v", sockAddr, req)
	reply, err := c.SyncupDn(ctx, req)
	if err != nil {
		logger.Warning("SyncupDn failed: %v", err)
	} else {
		logger.Info("SyncupDn reply: %v", reply)
	}
	return reply, err
}

func NewSyncupManager(kf *KeyFmt, sw *StmWrapper,
	createConn func(sockAddr string) (*grpc.ClientConn, error)) *SyncupManager {
	return &SyncupManager{
		kf:         kf,
		sw:         sw,
		createConn: createConn,
	}
}

func (sm *SyncupManager) SyncupCn(sockAddr string, ctx context.Context) {
	revision, controllerNode, err := sm.getControllerNode(sockAddr, ctx)
	if err != nil {
		return
	}
	req, err := sm.buildSyncupCnRequest(revision, controllerNode, ctx)
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
	}

	reply, err := sm.syncupCn(sockAddr, ctx, req)
	if err == nil {
		if reply.ReplyInfo.ReplyCode == CnSucceedCode {
			sm.getCnRsp(reply, idToRes)
			capDiffList := sm.setCnInfo(controllerNode, idToRes)
			sm.writeCnInfo(controllerNode, capDiffList, revision, ctx)
		} else {
			logger.Warning("SyncupCn reply error: %v", reply.ReplyInfo)
		}
	} else {
		logger.Warning("SyncupCn grpc error: %v", err)
		capDiffList := sm.setCnInfo(controllerNode, idToRes)
		sm.writeCnInfo(controllerNode, capDiffList, revision, ctx)
	}
}

func (sm *SyncupManager) getControllerNode(sockAddr string, ctx context.Context) (
	int64, *pbds.ControllerNode, error) {
	return int64(0), nil, nil
}

func (sm *SyncupManager) buildSyncupCnRequest(
	revision int64, controllerNode *pbds.ControllerNode, ctx context.Context) (
	*pbcn.SyncupCnRequest, error) {
	return nil, nil
}

func (sm *SyncupManager) syncupCn(sockAddr string, ctx context.Context,
	req *pbcn.SyncupCnRequest) (*pbcn.SyncupCnReply, error) {
	return nil, nil
}

func (sm *SyncupManager) getCnRsp(reply *pbcn.SyncupCnReply, idToRes *cnIdToRes) {
}

func (sm *SyncupManager) setCnInfo(controllerNode *pbds.ControllerNode,
	idToRes *cnIdToRes) []*capDiff {
	capDiffList := make([]*capDiff, 0)
	return capDiffList
}

func (sm *SyncupManager) writeCnInfo(controllerNode *pbds.ControllerNode,
	capDiffList []*capDiff, revision int64, ctx context.Context) {
}
