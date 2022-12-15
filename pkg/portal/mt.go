package portal

import (
	"context"
	"fmt"
	"time"

	// clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/protobuf/proto"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

func (po *portalServer) applyMovingTask(ctx context.Context,
	req *pbpo.CreateMtRequest) error {
	mtId := lib.NewHexStrUuid()
	dstVdId := lib.NewHexStrUuid()
	daEntityKey := po.kf.DaEntityKey(req.DaName)
	diskArray := &pbds.DiskArray{}
	dnEntityKey := po.kf.DnEntityKey(req.DstSockAddr)
	diskNode := &pbds.DiskNode{}
	apply := func(stm concurrency.STM) error {
		val := []byte(stm.Get(daEntityKey))
		if len(val) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				daEntityKey,
			}
		}
		if err := proto.Unmarshal(val, diskArray); err != nil {
			logger.Error("Unmarshal diskArray err: %s %v", daEntityKey, err)
			return err
		}
		var srcVd *pbds.VirtualDisk
		for _, grp := range diskArray.GrpList {
			if grp.GrpIdx == req.GrpIdx {
				for _, vd := range grp.VdList {
					if vd.VdIdx == req.VdIdx {
						srcVd = vd
						break
					}
				}
			}
		}
		if srcVd == nil {
			return &portalError{
				lib.PortalInvalidParamCode,
				"Can not find srcVd",
			}
		}
		for _, mt := range diskArray.MtList {
			if mt.MtName == req.MtName {
				return &portalError{
					lib.PortalDupResErrCode,
					req.MtName,
				}
			}
			if mt.GrpIdx == req.GrpIdx && mt.VdIdx == req.VdIdx {
				return &portalError{
					lib.PortalDupResErrCode,
					"Duplicate GrpIdx and VdIdx",
				}
			}
		}

		var primCntlr *pbds.Controller
		for _, cntlr := range diskArray.CntlrList {
			if cntlr.IsPrimary {
				primCntlr = cntlr
				break
			}
		}
		if primCntlr == nil {
			return &portalError{
				lib.PortalInternalErrCode,
				"Can not find primCntlr",
			}
		}
		mt := &pbds.MovingTask{
			MtId: mtId,
			MtName: req.MtName,
			Description: req.Description,
			GrpIdx: req.GrpIdx,
			VdIdx: req.VdIdx,
			SrcSockAddr: srcVd.DnSockAddr,
			SrcPdName: srcVd.PdName,
			SrcVdId: srcVd.VdId,
			DstSockAddr: req.DstSockAddr,
			DstPdName: req.DstPdName,
			DstVdId: dstVdId,
			Desc: &pbds.TaskDescriptor{
				StartTime: uint64(time.Now().UTC().Unix()),
				StopTime: 0,
				KeepSeconds: req.KeepSeconds,
				Status: lib.TaskStatusProcessing,
			},
		}
		diskArray.MtList = append(diskArray.MtList, mt)
		newDaEntityVal, err := proto.Marshal(diskArray)
		if err != nil {
			logger.Error("Marshal diskArray err: %v %v", diskArray, err)
			return fmt.Errorf("Marshal diskArray err: %v", err)
		}
		stm.Put(daEntityKey, string(newDaEntityVal))

		dnEntityVal := []byte(stm.Get(dnEntityKey))
		if len(dnEntityVal) == 0 {
			logger.Warning("Can not find diskNode: %v", req.DstSockAddr)
			msg := fmt.Sprintf("Can not find diskNode %s", req.DstSockAddr)
			return retriableError{msg}
		}
		if diskNode.SockAddr != req.DstSockAddr {
			logger.Warning("SockAddr mismatch: %v %v",
				req.DstSockAddr, diskNode)
			msg := fmt.Sprintf("SockAddr mismatch: %s %s",
				diskNode.SockAddr, req.DstSockAddr)
			return retriableError{msg}
		}
		if err := proto.Unmarshal(dnEntityVal, diskNode); err != nil {
			logger.Warning("Unmarshal diskNode err: %v %v", req.DstSockAddr, err)
			msg := fmt.Sprintf("Unmarshal diskNode err %s %v",
				req.DstSockAddr, err)
			return retriableError{msg}
		}
		if diskNode.DnInfo.ErrInfo.IsErr {
			logger.Warning("diskNode IsErr: %v", diskNode)
			msg := fmt.Sprintf("diskNode IsErr: %s", diskNode.SockAddr)
			return retriableError{msg}
		}
		if diskNode.DnConf.IsOffline {
			logger.Warning("diskNode IsOffline: %v", diskNode)
			msg := fmt.Sprintf("diskNode IsOffline: %s", diskNode.SockAddr)
			return retriableError{msg}
		}
		dstVd := &pbds.VirtualDisk{
			VdId: dstVdId,
			VdIdx: srcVd.VdIdx,
			Size: srcVd.Size,
			DnSockAddr: req.DstSockAddr,
			PdName: req.DstPdName,
			Qos: &pbds.BdevQos{
				RwIosPerSec:    srcVd.Qos.RwIosPerSec,
				RwMbytesPerSec: srcVd.Qos.RwMbytesPerSec,
				RMbytesPerSec:  srcVd.Qos.RMbytesPerSec,
				WMbytesPerSec:  srcVd.Qos.WMbytesPerSec,
			},
		}
		var dstPd *pbds.PhysicalDisk
		for _, pd := range diskNode.PdList {
			if pd.PdName == req.DstPdName {
				dstPd = pd
				break
			}
		}
		if dstPd == nil {
			logger.Warning("Can not find dstPd: %v %v", req.DstPdName, diskNode)
			msg := fmt.Sprintf("Can not find dstPd: %v %v", req.DstPdName, diskNode.SockAddr)
			return retriableError{msg}
		}
		if dstPd.PdInfo.ErrInfo.IsErr {
			logger.Warning("physicalDisk IsErr: %v %v", diskNode, dstPd)
			msg := fmt.Sprintf("physicalDisk IsErr: %s %s",
				diskNode.SockAddr, dstPd.PdName)
			return retriableError{msg}
		}
		if dstPd.PdConf.IsOffline {
			logger.Warning("physicalDisk IsOffline: %v %v", diskNode, dstPd)
			msg := fmt.Sprintf("physicalDisk IsOffline: %s %s",
				diskNode.SockAddr, dstPd.PdName)
			return retriableError{msg}
		}

		cap := dstPd.Capacity
		oldDnCapKey := po.kf.DnCapKey(cap.FreeSize, diskNode.SockAddr, dstPd.PdName)
		oldDnCapVal := stm.Get(oldDnCapKey)
		if len(oldDnCapVal) == 0 {
			logger.Warning("Can not find dn cap: %s %v", oldDnCapKey, diskNode)
			return fmt.Errorf("Can not find dn cap: %s %s",
				oldDnCapKey, diskNode.SockAddr)
		}
		stm.Del(oldDnCapKey)

		qos := dstVd.Qos
		if cap.FreeSize < dstVd.Size {
			logger.Warning("FreeSize not enough: %v %v %v",
				diskNode, cap, qos)
			msg := fmt.Sprintf("FreeSize not enough")
			return retriableError{msg}
		} else {
			cap.FreeSize -= dstVd.Size
		}

		if cap.FreeQos.RwIosPerSec != 0 && qos.RwIosPerSec != 0 {
			if cap.FreeQos.RwIosPerSec < qos.RwIosPerSec {
				logger.Warning("RwIosPerSec not enough: %v %v %v",
					diskNode, cap, qos)
				msg := fmt.Sprintf("RwIosPerSec not enough")
				return retriableError{msg}
			} else {
				cap.FreeQos.RwIosPerSec -= qos.RwIosPerSec
			}
		} else if cap.FreeQos.RwIosPerSec != 0 && qos.RwIosPerSec == 0 {
			logger.Warning("RwIosPerSec not enough: %v %v %v",
				diskNode, cap, qos)
			msg := fmt.Sprintf("RwIosPerSec not enough")
			return retriableError{msg}
		}

		if cap.FreeQos.RwMbytesPerSec != 0 && qos.RwMbytesPerSec != 0 {
			if cap.FreeQos.RwMbytesPerSec < qos.RwMbytesPerSec {
				logger.Warning("RwMbytesPerSec not enough: %v %v %v",
					diskNode, cap, qos)
				msg := fmt.Sprintf("RwMbytesPerSec not enough")
				return retriableError{msg}
			} else {
				cap.FreeQos.RwMbytesPerSec -= qos.RwMbytesPerSec
			}
		} else if cap.FreeQos.RwMbytesPerSec != 0 && qos.RwIosPerSec == 0 {
			logger.Warning("RwMbytesPerSec not enough: %v %v %v",
				diskNode, cap, qos)
			msg := fmt.Sprintf("RwMbytesPerSec not enough")
			return retriableError{msg}
		}

		if cap.FreeQos.RMbytesPerSec != 0 && qos.RMbytesPerSec != 0 {
			if cap.FreeQos.RMbytesPerSec < qos.RMbytesPerSec {
				logger.Warning("RMbytesPerSec not enough: %v %v %v",
					diskNode, cap, qos)
				msg := fmt.Sprintf("RMbytesPerSec not enough")
				return retriableError{msg}
			} else {
				cap.FreeQos.RMbytesPerSec -= qos.RMbytesPerSec
			}
		} else if cap.FreeQos.RMbytesPerSec != 0 && qos.RMbytesPerSec == 0 {
			logger.Warning("RMbytesPerSec not enough: %v %v %v",
				diskNode, cap, qos)
			msg := fmt.Sprintf("RMbytesPerSec not enough")
			return retriableError{msg}
		}

		if cap.FreeQos.WMbytesPerSec != 0 && qos.WMbytesPerSec != 0 {
			if cap.FreeQos.WMbytesPerSec < qos.WMbytesPerSec {
				logger.Warning("WMbytesPerSec not enough: %v %v %v",
					diskNode, cap, qos)
				msg := fmt.Sprintf("WMbytesPerSec not enough")
				return retriableError{msg}
			} else {
				cap.FreeQos.WMbytesPerSec -= qos.WMbytesPerSec
			}
		} else if cap.FreeQos.WMbytesPerSec != 0 && qos.WMbytesPerSec == 0 {
			logger.Warning("WMbytesPerSec not enough: %v %v %v",
				diskNode, cap, qos)
			msg := fmt.Sprintf("WMbytesPerSec not enough")
			return retriableError{msg}
		}

		newDnCapKey := po.kf.DnCapKey(cap.FreeSize, diskNode.SockAddr, dstPd.PdName)
		dnSearchAttr := &pbds.DnSearchAttr{
			PdCapacity: &pbds.PdCapacity{
				TotalSize: cap.TotalSize,
				FreeSize:  cap.FreeSize,
				TotalQos: &pbds.BdevQos{
					RwIosPerSec:    cap.TotalQos.RwIosPerSec,
					RwMbytesPerSec: cap.TotalQos.RwMbytesPerSec,
					RMbytesPerSec:  cap.TotalQos.RMbytesPerSec,
					WMbytesPerSec:  cap.TotalQos.WMbytesPerSec,
				},
				FreeQos: &pbds.BdevQos{
					RwIosPerSec:    cap.FreeQos.RwIosPerSec,
					RwMbytesPerSec: cap.FreeQos.RwMbytesPerSec,
					RMbytesPerSec:  cap.FreeQos.RMbytesPerSec,
					WMbytesPerSec:  cap.FreeQos.WMbytesPerSec,
				},
			},
			Location: diskNode.DnConf.Location,
		}
		newDnCapVal, err := proto.Marshal(dnSearchAttr)
		if err != nil {
			logger.Error("Marshal dnSearchAttr err: %v %v %v %v",
				diskNode, dstPd, dnSearchAttr, err)
			return fmt.Errorf("Marshal dnSearchAttr err: %v", err)
		}
		stm.Put(newDnCapKey, string(newDnCapVal))

		vdBe := &pbds.VdBackend{
			VdId: dstVd.VdId,
			VdBeConf: &pbds.VdBeConf{
				DaName: req.DaName,
				GrpIdx: req.GrpIdx,
				VdIdx: req.VdIdx,
				Size: dstVd.Size,
				Qos: &pbds.BdevQos{
					RwIosPerSec:    dstVd.Qos.RwIosPerSec,
					RwMbytesPerSec: dstVd.Qos.RwMbytesPerSec,
					RMbytesPerSec:  dstVd.Qos.RMbytesPerSec,
					WMbytesPerSec:  dstVd.Qos.WMbytesPerSec,
				},
				CntlrId: primCntlr.CntlrId,
			},
			VdBeInfo: &pbds.VdBeInfo{
				ErrInfo: &pbds.ErrInfo{
					IsErr: true,
					ErrMsg: lib.ResUninitMsg,
					Timestamp: lib.ResTimestamp(),
				},
			},
		}
		dstPd.VdBeList = append(dstPd.VdBeList, vdBe)
		diskNode.Version++
		newDnEntityVal, err := proto.Marshal(diskNode)
		if err != nil {
			logger.Error("Marshal diskNode err: %v %v", diskNode, err)
			return fmt.Errorf("Marshal diskNode err: %s %v",
				diskNode.SockAddr, err)
		}
		stm.Put(dnEntityKey, string(newDnEntityVal))

		dnErrKey := po.kf.DnErrKey(diskNode.DnConf.HashCode, diskNode.SockAddr)
		if len(stm.Get(dnErrKey)) == 0 {
			dnSummary := &pbds.DnSummary{
				SockAddr:    diskNode.SockAddr,
				Description: diskNode.DnConf.Description,
			}
			dnErrVal, err := proto.Marshal(dnSummary)
			if err != nil {
				logger.Error("Marshal dnSummary err: %v %v", dnSummary, err)
				return fmt.Errorf("Marshal dnSummary err: %s %v",
					diskNode.SockAddr, err)
			}
			stm.Put(dnErrKey, string(dnErrVal))
		}

		cnEntityKey := po.kf.CnEntityKey(primCntlr.CnSockAddr)
		cnEntityVal := []byte(stm.Get(cnEntityKey))
		if len(cnEntityVal) == 0 {
			logger.Warning("Can not find controllerNode, primCntlr: %v",
				primCntlr)
			return fmt.Errorf("Can not find controllerNode %s",
				primCntlr.CnSockAddr)
		}
		controllerNode := &pbds.ControllerNode{}
		if err := proto.Unmarshal(cnEntityVal, controllerNode); err != nil {
			logger.Warning("Unmarshal controllerNode err: %v %v %v",
				primCntlr, controllerNode, err)
			return fmt.Errorf("Unmarshal controllerNode err: %s %v",
				primCntlr.CnSockAddr, err)
		}
		if controllerNode.SockAddr != primCntlr.CnSockAddr {
			logger.Warning("SockAddr mismatch:  %v %v", primCntlr, controllerNode)
			return fmt.Errorf("SockAddr mismatch: %s %s",
				controllerNode.SockAddr, primCntlr.CnSockAddr)
		}
		var thisCntlrFe *pbds.CntlrFrontend
		for _, cntlrFe := range controllerNode.CntlrFeList {
			if cntlrFe.CntlrId == primCntlr.CntlrId {
				thisCntlrFe = cntlrFe
				break
			}
		}
		if thisCntlrFe == nil {
			logger.Warning("Can not find cntlr: %v %v",
				controllerNode, primCntlr)
			return fmt.Errorf("Can not find cntlr: %s %s",
				controllerNode.SockAddr, primCntlr.CntlrId)
		}
		var srcVdFe *pbds.VdFrontend
		for _, grpFe := range thisCntlrFe.GrpFeList {
			if grpFe.GrpFeConf.GrpIdx == req.GrpIdx {
				for _, vdFe := range grpFe.VdFeList {
					if vdFe.VdFeConf.VdIdx == req.VdIdx {
						srcVdFe = vdFe
						break
					}
				}
			}
		}
		if srcVdFe == nil {
			logger.Warning("Can not find srcVdFe")
			return fmt.Errorf("Can not find srcVdFe")
		}
		mtFe := &pbds.MtFrontend{
			MtId: mtId,
			MtFeConf: &pbds.MtFeConf{
				GrpIdx: req.GrpIdx,
				VdIdx: req.VdIdx,
				SrcListener: &pbds.NvmfListener{
					TrType: srcVdFe.VdFeConf.DnNvmfListener.TrType,
					AdrFam: srcVdFe.VdFeConf.DnNvmfListener.AdrFam,
					TrAddr: srcVdFe.VdFeConf.DnNvmfListener.TrAddr,
					TrSvcId: srcVdFe.VdFeConf.DnNvmfListener.TrSvcId,
				},
				SrcVdId: srcVd.VdId,
				DstListener: &pbds.NvmfListener{
					TrType: diskNode.DnConf.NvmfListener.TrType,
					AdrFam: diskNode.DnConf.NvmfListener.AdrFam,
					TrAddr: diskNode.DnConf.NvmfListener.TrAddr,
					TrSvcId: diskNode.DnConf.NvmfListener.TrSvcId,
				},
				DstVdId: dstVd.VdId,
			},
			MtFeInfo: &pbds.MtFeInfo{
				ErrInfo: &pbds.ErrInfo{
					IsErr:     true,
					ErrMsg:    lib.ResUninitMsg,
					Timestamp: lib.ResTimestamp(),
				},
			},
		}
		thisCntlrFe.MtFeList = append(thisCntlrFe.MtFeList, mtFe)
		controllerNode.Version++
		newCnEntityVal, err := proto.Marshal(controllerNode)
		if err != nil {
			logger.Error("Marshal controllerNode err: %v %v", controllerNode, err)
			return fmt.Errorf("Marshal controllerNode err: %s %v",
				controllerNode.SockAddr, err)
		}
		stm.Put(cnEntityKey, string(newCnEntityVal))

		cnErrKey := po.kf.CnErrKey(controllerNode.CnConf.HashCode, controllerNode.SockAddr)
		if len(stm.Get(cnErrKey)) == 0 {
			cnSummary := &pbds.CnSummary{
				SockAddr:    controllerNode.SockAddr,
				Description: controllerNode.CnConf.Description,
			}
			cnErrVal, err := proto.Marshal(cnSummary)
			if err != nil {
				logger.Error("Marshal cnSummary err: %v %v", cnSummary, err)
				return fmt.Errorf("Marshal cnSummary err: %s %v",
					controllerNode.SockAddr, err)
			}
			stm.Put(cnErrKey, string(cnErrVal))
		}
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "CreateMt: "+req.DaName+" "+req.MtName)
	return err
}

func (po *portalServer) addNewLeg(ctx context.Context, req *pbpo.CreateMtRequest)(
	[]string, []string, error) {
	dnList := make([]string, 0)
	cnList := make([]string, 0)

	session, err := concurrency.NewSession(po.etcdCli,
		concurrency.WithTTL(lib.AllocLockTTL))
	if err != nil {
		logger.Error("Create session err: %v", err)
		return dnList, cnList, err
	}
	defer session.Close()
	mutex := concurrency.NewMutex(session, po.kf.AllocLockPath())
	if err = mutex.Lock(ctx); err != nil {
		logger.Error("Lock mutex err: %v", err)
		return dnList, cnList, err
	}

	defer func() {
		if err := mutex.Unlock(ctx); err != nil {
			logger.Error("Unklock mutex err: %v", err)
		}
	}()

	var vdSize uint64
	var qos *lib.BdevQos
	var cnSockAddr string
	daEntityKey := po.kf.DaEntityKey(req.DaName)
	diskArray := &pbds.DiskArray{}
	apply := func(stm concurrency.STM) error {
		val := []byte(stm.Get(daEntityKey))
		if len(val) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				daEntityKey,
			}
		}
		if err := proto.Unmarshal(val, diskArray); err != nil {
			logger.Error("Unmarshal diskArray err: %s %v", daEntityKey, err)
			return err
		}
		if int(req.GrpIdx) >= len(diskArray.GrpList) {
			return &portalError{
				lib.PortalInvalidParamCode,
				"GrpIdx out of scope",
			}
		}
		var srcVd *pbds.VirtualDisk
		for _, grp := range diskArray.GrpList {
			if grp.GrpIdx == req.GrpIdx {
				for _, vd := range grp.VdList {
					if vd.VdIdx == req.VdIdx {
						srcVd = vd
						break
					}
				}
			}
		}
		if srcVd == nil {
			return &portalError{
				lib.PortalInvalidParamCode,
				"Can not find srcVd",
			}
		}
		vdSize = srcVd.Size
		qos = &lib.BdevQos{
			RwIosPerSec: srcVd.Qos.RwIosPerSec,
			RwMbytesPerSec: srcVd.Qos.RwMbytesPerSec,
			RMbytesPerSec: srcVd.Qos.RMbytesPerSec,
			WMbytesPerSec: srcVd.Qos.WMbytesPerSec,
		}
		for _, cntlr := range diskArray.CntlrList {
			if cntlr.IsPrimary {
				cnSockAddr = cntlr.CnSockAddr
				break
			}
		}
		return nil
	}
	err = po.sw.RunStm(apply, ctx, "MtGetDa: "+req.DaName+" "+req.MtName)
	if err != nil {
		return dnList, cnList, err
	}

	retryCnt := 0
	maxRetry := lib.AllocMaxRetry
	if req.DstSockAddr != "" {
		maxRetry = 1
	}
	for {
		retryCnt++
		if retryCnt > maxRetry {
			err = fmt.Errorf("Exceed max retry cnt")
			logger.Error("Exceed max retry cnt")
			return dnList, cnList, err
		}

		if req.DstSockAddr == "" {
			dnPdCandList, err := po.alloc.AllocDnPd(ctx, 1, vdSize, qos)
			if err != nil {
				logger.Error("AllocateDnPd err: %v", err)
				return dnList, cnList, err
			}
			req.DstSockAddr = dnPdCandList[0].SockAddr
			req.DstPdName = dnPdCandList[0].PdName
		}
		err = po.applyMovingTask(ctx, req)
		if err != nil {
			if serr, ok := err.(*retriableError); ok {
				logger.Warning("Retriable error: %v", serr)
				continue
			} else {
				logger.Error("applyMovingTask err: %v", err)
				return dnList, cnList, err
			}
		}
		dnList = make([]string, 0)
		cnList = make([]string, 0)
		dnList = append(dnList, req.DstSockAddr)
		cnList = append(cnList, cnSockAddr)
		return dnList, cnList, nil
	}
}

func (po *portalServer) CreateMt(ctx context.Context, req *pbpo.CreateMtRequest)(
	*pbpo.CreateMtReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	} else if req.MtName == "" {
		invalidParamMsg = "MtName is empty"
	} else if req.DstSockAddr == "" && req.DstPdName != "" {
		invalidParamMsg = "DstSockAddr and DstPdName must be specified together"
	} else if req.DstSockAddr != "" && req.DstPdName == "" {
		invalidParamMsg = "DstSockAddr and DstPdName must be specified together"
	}
	if invalidParamMsg != "" {
		return &pbpo.CreateMtReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId: lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg: invalidParamMsg,
			},
		}, nil
	}

	if req.KeepSeconds == 0 {
		req.KeepSeconds = lib.DefaultKeepSeconds
	}

	dnList, cnList, err := po.addNewLeg(ctx, req)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.CreateMtReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId: lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg: serr.msg,
				},
			}, nil
		} else {
			return &pbpo.CreateMtReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId: lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg: err.Error(),
				},
			}, nil
		}
	}

	for _, sockAddr := range dnList {
		po.sm.SyncupDn(sockAddr, ctx)
	}
	for _, sockAddr := range cnList {
		po.sm.SyncupCn(sockAddr, ctx)
	}
	return &pbpo.CreateMtReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId: lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg: lib.PortalSucceedMsg,
		},
	}, nil
}
