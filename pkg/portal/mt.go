package portal

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/protobuf/proto"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

func (po *portalServer) applyMovingTask(ctx context.Context,
	req *pbpo.CreateMtRequest, check_status bool) error {
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
			if mt.GrpIdx == req.GrpIdx &&
				mt.VdIdx == req.VdIdx &&
				mt.Desc.Status == lib.TaskStatusProcessing {
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
			Raid1Conf: &pbds.Raid1Conf{
				BitSizeKb: req.Raid1Conf.BitSizeKb,
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
			logger.Warning("Unmarshal diskNode err: %v %v",
				dnEntityVal, err)
			msg := fmt.Sprintf("Unmarshal diskNode err %s %v",
				req.DstSockAddr, err)
			return retriableError{msg}
		}
		if check_status {
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
		if check_status {
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
				Raid1Conf: &pbds.Raid1Conf{
					BitSizeKb: req.Raid1Conf.BitSizeKb,
				},
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

		check_status := false
		if req.DstSockAddr == "" {
			dnLocList := make([]string, 0)
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
				var targetGrp *pbds.Group
				for _, grp := range diskArray.GrpList {
					if grp.GrpIdx == req.GrpIdx {
						targetGrp = grp
						break
					}
				}
				if targetGrp == nil {
					return &portalError{
						lib.PortalUnknownResErrCode,
						fmt.Sprintf("GrpIdx: %d", req.GrpIdx),
					}
				}
				for _, vd := range targetGrp.VdList {
					if vd.VdIdx == req.VdIdx {
						continue
					}
					dnEntityKey := po.kf.DnEntityKey(vd.DnSockAddr)
					dnEntityVal := []byte(stm.Get(dnEntityKey))
					if len(dnEntityVal) == 0 {
						logger.Warning("Can not find diskNode: %v",
							vd.DnSockAddr)
						return &portalError{
							lib.PortalUnknownResErrCode,
							fmt.Sprintf("Dn: %s", vd.DnSockAddr),
						}
					}
					diskNode := &pbds.DiskNode{}
					if err := proto.Unmarshal(dnEntityVal, diskNode); err != nil {
						logger.Warning("Unmarshal diskNode err: %v %v",
							dnEntityVal, err)
						return fmt.Errorf("Unmarshal diskNode err: %s %v",
							vd.DnSockAddr, err)
					}
					dnLocList = make([]string, 0)
					dnLocList = append(dnLocList,
						diskNode.DnConf.Location)
				}
				return nil
			}
			err := po.sw.RunStm(apply, ctx, "CreateMtGetLoc: "+req.DaName+" "+req.MtName)
			if err != nil {
				return dnList, cnList, err
			}
			dnPdCandList, err := po.alloc.AllocDnPd(
				ctx, 1, vdSize, qos, dnLocList)
			if err != nil {
				logger.Error("AllocateDnPd err: %v", err)
				return dnList, cnList, err
			}
			req.DstSockAddr = dnPdCandList[0].SockAddr
			req.DstPdName = dnPdCandList[0].PdName
			check_status = true
		}
		err = po.applyMovingTask(ctx, req, check_status)
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
	if req.Raid1Conf == nil {
		req.Raid1Conf = &pbpo.Raid1Conf{
			BitSizeKb: lib.DefaultBitSizeKb,
		}
	}
	if req.Raid1Conf.BitSizeKb == 0 {
		req.Raid1Conf.BitSizeKb = lib.DefaultBitSizeKb
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

func (po *portalServer) cancelMt(ctx context.Context, req *pbpo.CancelMtRequest) (
	string, string, error) {
	daEntityKey := po.kf.DaEntityKey(req.DaName)
	diskArray := &pbds.DiskArray{}
	primaryCnSockAddr := ""
	dstDnSockAddr := ""

	apply := func(stm concurrency.STM) error {
		daEntityVal := []byte(stm.Get(daEntityKey))
		if len(daEntityVal) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				daEntityKey,
			}
		}
		if err := proto.Unmarshal(daEntityVal, diskArray); err != nil {
			logger.Error("Unmarshal diskArray err: %s %v", daEntityKey, err)
			return err
		}
		var targetMt *pbds.MovingTask
		for _, mt := range diskArray.MtList {
			if mt.MtName == req.MtName {
				targetMt = mt
				break
			}
		}
		if targetMt == nil {
			return &portalError{
				lib.PortalUnknownResErrCode,
				req.MtName,
			}
		}
		if targetMt.Desc.Status != lib.TaskStatusProcessing {
			return fmt.Errorf("Can not cancel Mt in status %s",
				targetMt.Desc.Status)
		}
		targetMt.Desc.Status = lib.TaskStatusCanceled
		newDaEntityVal, err := proto.Marshal(diskArray)
		if err != nil {
			logger.Error("Marshal diskArray err: %v %v", diskArray, err)
			return err
		}
		stm.Put(daEntityKey, string(newDaEntityVal))

		var primaryCntlr *pbds.Controller
		for _, cntlr := range diskArray.CntlrList {
			if cntlr.IsPrimary {
				primaryCntlr = cntlr
				break
			}
		}
		if primaryCntlr == nil {
			logger.Error("Can not find primary cntlr")
			return fmt.Errorf("Can not find primary cntlr")
		}
		primaryCnSockAddr = primaryCntlr.CnSockAddr
		controllerNode := &pbds.ControllerNode{}
		cnEntityKey := po.kf.CnEntityKey(primaryCntlr.CnSockAddr)
		cnEntityVal := []byte(stm.Get(cnEntityKey))
		if err := proto.Unmarshal(cnEntityVal, controllerNode); err != nil {
			logger.Error("Unmarshal controllerNode err: %s %v",
				cnEntityKey, err)
			return err
		}
		var targetCntlrFe *pbds.CntlrFrontend
		for _, cntlrFe := range controllerNode.CntlrFeList {
			if cntlrFe.CntlrId == primaryCntlr.CntlrId {
				targetCntlrFe = cntlrFe
				break
			}
		}
		if targetCntlrFe == nil {
			logger.Error("Can not find cntlr: %v %v",
				primaryCntlr, controllerNode)
			return fmt.Errorf("Can not find cntlr: %s %s",
				primaryCntlr.CntlrId, controllerNode.SockAddr)
		}
		targetIdx := -1
		for i, mtFe := range targetCntlrFe.MtFeList {
			if mtFe.MtId == targetMt.MtId {
				targetIdx = i
				break
			}
		}
		if targetIdx == -1 {
			logger.Error("Can not find mtFe: %v %v",
				targetMt, controllerNode)
			return fmt.Errorf("Can not find mtFe: %s %s",
				targetMt.MtName, controllerNode.SockAddr)
		}
		length := len(targetCntlrFe.MtFeList)
		targetCntlrFe.MtFeList[targetIdx] = targetCntlrFe.MtFeList[length-1]
		targetCntlrFe.MtFeList = targetCntlrFe.MtFeList[:length-1]
		controllerNode.Version++
		newCnEntityVal, err := proto.Marshal(controllerNode)
		if err != nil {
			logger.Error("Marshal controllerNode err: %v %v", controllerNode, err)
			return fmt.Errorf("Marshal controllerNode err: %v", err)
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

		dstDnSockAddr = targetMt.DstSockAddr
		dnEntityKey := po.kf.DnEntityKey(targetMt.DstSockAddr)
		dnEntityVal := []byte(stm.Get(dnEntityKey))
		if len(dnEntityVal) == 0 {
			logger.Error("Can not find dn: %s", dnEntityKey)
			return &portalError{
				lib.PortalInternalErrCode,
				fmt.Sprintf("Can not find dn: %s", targetMt.DstSockAddr),
			}
		}
		diskNode := &pbds.DiskNode{}
		if err := proto.Unmarshal(dnEntityVal, diskNode); err != nil {
			logger.Error("Unmarshal diskNode err: %s %v", dnEntityVal, err)
			return err
		}
		var targetPd *pbds.PhysicalDisk
		for _, pd := range diskNode.PdList {
			if pd.PdName == targetMt.DstPdName {
				targetPd = pd
				break
			}
		}
		if targetPd == nil {
			logger.Error("Can not find pd: %v %v", diskNode, targetMt)
			return &portalError{
				lib.PortalInternalErrCode,
				"Can not find pd",
			}
		}
		var targetVdBe *pbds.VdBackend
		targetIdx = -1
		for i, vdBe := range targetPd.VdBeList {
			if vdBe.VdId == targetMt.DstVdId {
				targetVdBe = vdBe
				targetIdx = i
				break
			}
		}
		if targetIdx == -1 {
			logger.Error("Can not find vd: %v %v", diskNode, targetMt)
			return &portalError{
				lib.PortalInternalErrCode,
				"Can not find vd",
			}
		}
		length = len(targetPd.VdBeList)
		targetPd.VdBeList[targetIdx] = targetPd.VdBeList[length-1]
		targetPd.VdBeList = targetPd.VdBeList[:length-1]

		cap := targetPd.Capacity
		oldDnCapKey := po.kf.DnCapKey(cap.FreeSize, diskNode.SockAddr, targetPd.PdName)
		cap.FreeSize += targetVdBe.VdBeConf.Size
		cap.FreeQos.RwIosPerSec += targetVdBe.VdBeConf.Qos.RwIosPerSec
		cap.FreeQos.RwMbytesPerSec += targetVdBe.VdBeConf.Qos.RwMbytesPerSec
		cap.FreeQos.RMbytesPerSec += targetVdBe.VdBeConf.Qos.RMbytesPerSec
		cap.FreeQos.WMbytesPerSec += targetVdBe.VdBeConf.Qos.WMbytesPerSec
		newDnCapKey := po.kf.DnCapKey(cap.FreeSize, diskNode.SockAddr, targetPd.PdName)
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
		dnCapVal, err := proto.Marshal(dnSearchAttr)
		if err != nil {
			logger.Error("marshal dnSearchAttr err: %v %v",
				dnSearchAttr, err)
			return err
		}
		stm.Del(oldDnCapKey)
		stm.Put(newDnCapKey, string(dnCapVal))
		diskNode.Version++
		newDnEntityVal, err := proto.Marshal(diskNode)
		if err != nil {
			logger.Error("Marshal diskNode err: %s %v %v",
				dnEntityKey, diskNode, err)
			return err
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
				logger.Error("Marshal dnSummary err: %v %v",
					dnSummary, err)
				return err
			}
			stm.Put(dnErrKey, string(dnErrVal))
		}
		return nil
	}

	session, err := concurrency.NewSession(po.etcdCli,
		concurrency.WithTTL(lib.AllocLockTTL))
	if err != nil {
		logger.Error("Create session err: %v", err)
		return primaryCnSockAddr, dstDnSockAddr, err
	}
	defer session.Close()
	mutex := concurrency.NewMutex(session, po.kf.AllocLockPath())
	if err = mutex.Lock(ctx); err != nil {
		logger.Error("Lock mutex err: %v", err)
		return primaryCnSockAddr, dstDnSockAddr, err
	}
	defer func() {
		if err := mutex.Unlock(ctx); err != nil {
			logger.Error("Unlock mutex err: %v", err)
		}
	}()

	err = po.sw.RunStm(apply, ctx, "CancelMt: "+req.DaName)
	return primaryCnSockAddr, dstDnSockAddr, err
}

func (po *portalServer) CancelMt(ctx context.Context, req *pbpo.CancelMtRequest)(
	*pbpo.CancelMtReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	} else if req.MtName == "" {
		invalidParamMsg = "MtName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.CancelMtReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId: lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg: invalidParamMsg,
			},
		}, nil
	}

	primaryCnSockAddr, dstDnSockAddr, err := po.cancelMt(ctx, req)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.CancelMtReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			po.sm.SyncupCn(primaryCnSockAddr, ctx)
			po.sm.SyncupDn(dstDnSockAddr, ctx)
			return &pbpo.CancelMtReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.CancelMtReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
		}, nil
	}
}

func (po *portalServer) modifyMtDescription(ctx context.Context, daName string,
	mtName string, description string) (*pbpo.ModifyMtReply, error) {
	daEntityKey := po.kf.DaEntityKey(daName)
	diskArray := &pbds.DiskArray{}

	apply := func(stm concurrency.STM) error {
		daEntityVal := []byte(stm.Get(daEntityKey))
		if len(daEntityVal) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				daEntityKey,
			}
		}
		if err := proto.Unmarshal(daEntityVal, diskArray); err != nil {
			logger.Error("Unmarshal diskArray err: %s %v",
				daEntityKey, err)
			return err
		}
		var targetMt *pbds.MovingTask
		for _, mt := range diskArray.MtList {
			if mt.MtName == mtName {
				targetMt = mt
				break
			}
		}
		if targetMt == nil {
			return &portalError{
				lib.PortalUnknownResErrCode,
				mtName,
			}
		}
		targetMt.Description = description
		newDaEntityVal, err := proto.Marshal(diskArray)
		if err != nil {
			logger.Error("Marshal diskArray err: %v %v", diskArray, err)
			return err
		}
		stm.Put(daEntityKey, string(newDaEntityVal))
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "ModifyMt: "+daName+" "+mtName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.ModifyMtReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.ModifyMtReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	}

	return &pbpo.ModifyMtReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}

func (po *portalServer) modifyMtKeepSeconds(ctx context.Context, daName string,
	mtName string, keepSeconds uint64) (*pbpo.ModifyMtReply, error) {
	daEntityKey := po.kf.DaEntityKey(daName)
	diskArray := &pbds.DiskArray{}

	apply := func(stm concurrency.STM) error {
		daEntityVal := []byte(stm.Get(daEntityKey))
		if len(daEntityVal) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				daEntityKey,
			}
		}
		if err := proto.Unmarshal(daEntityVal, diskArray); err != nil {
			logger.Error("Unmarshal diskArray err: %s %v",
				daEntityKey, err)
			return err
		}
		var targetMt *pbds.MovingTask
		for _, mt := range diskArray.MtList {
			if mt.MtName == mtName {
				targetMt = mt
				break
			}
		}
		if targetMt == nil {
			return &portalError{
				lib.PortalUnknownResErrCode,
				mtName,
			}
		}
		targetMt.Desc.KeepSeconds = keepSeconds
		newDaEntityVal, err := proto.Marshal(diskArray)
		if err != nil {
			logger.Error("Marshal diskArray err: %v %v", diskArray, err)
			return err
		}
		stm.Put(daEntityKey, string(newDaEntityVal))
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "ModifyMt: "+daName+" "+mtName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.ModifyMtReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.ModifyMtReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	}

	return &pbpo.ModifyMtReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}

func (po *portalServer) ModifyMt(ctx context.Context, req *pbpo.ModifyMtRequest) (
	*pbpo.ModifyMtReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	} else if req.MtName == "" {
		invalidParamMsg = "MtName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.ModifyMtReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId: lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg: invalidParamMsg,
			},
		}, nil
	}

	switch x := req.Attr.(type) {
	case *pbpo.ModifyMtRequest_Description:
		return po.modifyMtDescription(
			ctx, req.DaName, req.MtName, x.Description)
	case *pbpo.ModifyMtRequest_KeepSeconds:
		return po.modifyMtKeepSeconds(
			ctx, req.DaName, req.MtName, x.KeepSeconds)
	default:
		logger.Error("Unknow attr: %v", x)
		return &pbpo.ModifyMtReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  "Unknow attr",
			},
		}, nil
	}
}

func (po *portalServer) ListMt(ctx context.Context, req *pbpo.ListMtRequest) (
	*pbpo.ListMtReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.ListMtReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	daEntityKey := po.kf.DaEntityKey(req.DaName)
	diskArray := &pbds.DiskArray{}
	mtSummaryList := make([]*pbpo.MtSummary, 0)
	apply := func(stm concurrency.STM) error {
		dnEntityVal := []byte(stm.Get(daEntityKey))
		if len(dnEntityVal) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				daEntityKey,
			}
		}
		if err := proto.Unmarshal(dnEntityVal, diskArray); err != nil {
			logger.Error("Unmarshal diskArray err: %s %v",
				daEntityKey, err)
			return err
		}
		mtSummaryList = make([]*pbpo.MtSummary, 0)
		for _, mt := range diskArray.MtList {
			mtSummary := &pbpo.MtSummary{
				MtName: mt.MtName,
				Description: mt.Description,
			}
			mtSummaryList = append(mtSummaryList, mtSummary)
		}
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "ListMt: "+req.DaName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.ListMtReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.ListMtReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.ListMtReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
			MtSummaryList: mtSummaryList,
		}, nil
	}
}

func (po *portalServer) GetMt(ctx context.Context, req *pbpo.GetMtRequest) (
	*pbpo.GetMtReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	} else if req.MtName == "" {
		invalidParamMsg = "MtName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.GetMtReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	daEntityKey := po.kf.DaEntityKey(req.DaName)
	diskArray := &pbds.DiskArray{}
	var movingTask *pbpo.MovingTask

	apply := func(stm concurrency.STM) error {
		daEntityVal := []byte(stm.Get(daEntityKey))
		if len(daEntityVal) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				daEntityKey,
			}
		}
		if err := proto.Unmarshal(daEntityVal, diskArray); err != nil {
			logger.Error("Unmarshal diskArray err: %s %v",
				daEntityKey, err)
			return err
		}
		var targetMt *pbds.MovingTask
		for _, mt := range diskArray.MtList {
			if mt.MtName == req.MtName {
				targetMt = mt
				break
			}
		}
		if targetMt == nil {
			return &portalError{
				lib.PortalUnknownResErrCode,
				req.MtName,
			}
		}

		var targetCntlr *pbds.Controller
		for _, cntlr := range diskArray.CntlrList {
			if cntlr.IsPrimary {
				targetCntlr = cntlr
				break
			}
		}
		if targetCntlr == nil {
			logger.Error("Can not find primary cntlr: %v", diskArray)
			return fmt.Errorf("Can not find primary cntlr: %s",
				req.DaName)
		}
		controllerNode := &pbds.ControllerNode{}
		cnEntityKey := po.kf.CnEntityKey(targetCntlr.CnSockAddr)
		cnEntityVal := []byte(stm.Get(cnEntityKey))
		if err := proto.Unmarshal(cnEntityVal, controllerNode); err != nil {
			logger.Error("Unmarshal controllerNode err: %s %v",
				cnEntityKey, err)
			return err
		}
		var targetCntlrFe *pbds.CntlrFrontend
		for _, cntlrFe := range controllerNode.CntlrFeList {
			if cntlrFe.CntlrId == targetCntlr.CntlrId {
				targetCntlrFe = cntlrFe
				break
			}
		}
		if targetCntlrFe == nil {
			logger.Error("Can not find cntlr: %v %v",
				targetCntlr, controllerNode)
			return fmt.Errorf("Can not find cntlr")
		}
		var targetMtFe *pbds.MtFrontend
		for _, mtFe := range targetCntlrFe.MtFeList {
			if mtFe.MtId == targetMt.MtId {
				targetMtFe = mtFe
				break
			}
		}
		if targetMtFe == nil {
			logger.Error("Can not find mtFe: %v %v",
				targetMt, controllerNode)
			return fmt.Errorf("Can not find mtFe: %s %s",
				targetMt.MtId, controllerNode.SockAddr)
		}
		movingTask = &pbpo.MovingTask{
			MtId: targetMt.MtId,
			MtName: targetMt.MtName,
			Description: targetMt.Description,
			GrpIdx: targetMt.GrpIdx,
			VdIdx: targetMt.VdIdx,
			SrcSockAddr: targetMt.SrcSockAddr,
			SrcPdName: targetMt.SrcPdName,
			SrcVdId: targetMt.SrcVdId,
			DstSockAddr: targetMt.DstSockAddr,
			DstPdName: targetMt.DstPdName,
			DstVdId: targetMt.DstVdId,
			Desc: &pbpo.TaskDescriptor{
				StartTime: targetMt.Desc.StartTime,
				StopTime: targetMt.Desc.StopTime,
				KeepSeconds: targetMt.Desc.KeepSeconds,
				Status: targetMt.Desc.Status,
			},
			ErrInfo: &pbpo.ErrInfo{
				IsErr: targetMtFe.MtFeInfo.ErrInfo.IsErr,
				ErrMsg: targetMtFe.MtFeInfo.ErrInfo.ErrMsg,
				Timestamp: targetMtFe.MtFeInfo.ErrInfo.Timestamp,
			},
			Raid1Conf: &pbpo.Raid1Conf{
				BitSizeKb: targetMtFe.MtFeConf.Raid1Conf.BitSizeKb,
			},
		}
		if targetMtFe.MtFeInfo.Raid1Info != nil {
			movingTask.Raid1Info = &pbpo.Raid1Info{
				Bdev0Online: targetMtFe.MtFeInfo.Raid1Info.Bdev0Online,
				Bdev1Online: targetMtFe.MtFeInfo.Raid1Info.Bdev1Online,
				TotalBit: targetMtFe.MtFeInfo.Raid1Info.TotalBit,
				SyncedBit: targetMtFe.MtFeInfo.Raid1Info.SyncedBit,
				ResyncIoCnt: targetMtFe.MtFeInfo.Raid1Info.ResyncIoCnt,
				Status: targetMtFe.MtFeInfo.Raid1Info.Status,
			}
		}
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "GetMt: "+req.DaName+" "+req.MtName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.GetMtReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.GetMtReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.GetMtReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
			MovingTask: movingTask,
		}, nil
	}
}
