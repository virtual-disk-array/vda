package portal

import (
	"context"
	"fmt"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/golang/protobuf/proto"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

type retriableError struct {
	msg string
}

func (e retriableError) Error() string {
	return e.msg
}

func (po *portalServer) applyAllocation(ctx context.Context, req *pbpo.CreateDaRequest,
	dnPdCandList []*lib.DnPdCand, cnCandList []*lib.CnCand,
	qos *lib.BdevQos, vdSize uint64) error {

	daId := lib.NewHexStrUuid()
	grpId := lib.NewHexStrUuid()
	snapId := lib.NewHexStrUuid()
	grpSize := vdSize * uint64(req.StripCnt)

	apply := func(stm concurrency.STM) error {
		dnList := make([]*pbds.DiskNode, 0)
		vdList := make([]*pbds.VirtualDisk, 0)
		for i, cand := range dnPdCandList {
			dnEntityKey := po.kf.DnEntityKey(cand.SockAddr)
			dnEntityVal := []byte(stm.Get(dnEntityKey))
			if len(dnEntityVal) == 0 {
				logger.Warning("Can not find diskNode, cand: %v", cand)
				msg := fmt.Sprintf("Can not find diskNode %s", cand.SockAddr)
				return retriableError{msg}
			}
			diskNode := &pbds.DiskNode{}
			if err := proto.Unmarshal(dnEntityVal, diskNode); err != nil {
				logger.Warning("Unmarshal diskNode err: %v %v %v", cand, diskNode, err)
				msg := fmt.Sprintf("Unmarshal diskNode err %s %v",
					cand.SockAddr, err)
				return retriableError{msg}
			}
			if diskNode.SockAddr != cand.SockAddr {
				logger.Warning("SockAddr mismatch: %v %v", cand, diskNode)
				msg := fmt.Sprintf("SockAddr mismatch: %s %s",
					diskNode.SockAddr, cand.SockAddr)
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
			dnList = append(dnList, diskNode)

			vd := &pbds.VirtualDisk{
				VdId:       lib.NewHexStrUuid(),
				VdIdx:      uint32(i),
				Size:       vdSize,
				DnSockAddr: cand.SockAddr,
				PdName:     cand.PdName,
				Qos: &pbds.BdevQos{
					RwIosPerSec:    qos.RwIosPerSec,
					RwMbytesPerSec: qos.RwMbytesPerSec,
					RMbytesPerSec:  qos.RMbytesPerSec,
					WMbytesPerSec:  qos.WMbytesPerSec,
				},
			}
			vdList = append(vdList, vd)
		}

		cnList := make([]*pbds.ControllerNode, 0)
		for _, cand := range cnCandList {
			cnEntityKey := po.kf.CnEntityKey(cand.SockAddr)
			cnEntityVal := []byte(stm.Get(cnEntityKey))
			if len(cnEntityVal) == 0 {
				logger.Warning("Can not find controllerNode, cand: %v", cand)
				msg := fmt.Sprintf("Can not find controllerNode %s", cand.SockAddr)
				return retriableError{msg}
			}
			controllerNode := &pbds.ControllerNode{}
			if err := proto.Unmarshal(cnEntityVal, controllerNode); err != nil {
				logger.Warning("Unmarshal controllerNode err: %v %v %v",
					cand, controllerNode, err)
				msg := fmt.Sprintf("Unmarshal controllerNode err: %s %v",
					cand.SockAddr, err)
				return retriableError{msg}
			}
			if controllerNode.SockAddr != cand.SockAddr {
				logger.Warning("SockAddr mismatch:  %v %v", cand, controllerNode)
				msg := fmt.Sprintf("SockAddr mismatch: %s %s",
					controllerNode.SockAddr, cand.SockAddr)
				logger.Warning(msg)
				return retriableError{msg}
			}
			if controllerNode.CnInfo.ErrInfo.IsErr {
				logger.Warning("controllerNode IsErr: %v", controllerNode)
				msg := fmt.Sprintf("controllerNode IsErr: %s", controllerNode.SockAddr)
				return retriableError{msg}
			}
			if controllerNode.CnConf.IsOffline {
				logger.Warning("controllerNode IsOffline: %v", controllerNode.CnConf.IsOffline)
				msg := fmt.Sprintf("controllerNode IsOffline: %s", controllerNode.SockAddr)
				return retriableError{msg}
			}
			cnList = append(cnList, controllerNode)
		}

		grpList := make([]*pbds.Group, 0)
		grp := &pbds.Group{
			GrpId:  grpId,
			GrpIdx: uint32(0),
			Size:   grpSize,
			VdList: vdList,
		}
		grpList = append(grpList, grp)

		snapList := make([]*pbds.Snap, 0)
		snap := &pbds.Snap{
			SnapId:      snapId,
			SnapName:    lib.DefaultSanpName,
			Description: lib.DefaultSanpDescription,
			OriName:     "",
			IsClone:     false,
			Idx:         0,
			Size:        req.Size,
		}
		snapList = append(snapList, snap)

		expList := make([]*pbds.Exporter, 0)

		vdFeList := make([]*pbds.VdFrontend, 0)
		for _, vd := range vdList {
			vdFe := &pbds.VdFrontend{
				VdId: vd.VdId,
				VdFeConf: &pbds.VdFeConf{
					DnNvmfListener: &pbds.NvmfListener{
						TrType:  dnList[vd.VdIdx].DnConf.NvmfListener.TrType,
						AdrFam:  dnList[vd.VdIdx].DnConf.NvmfListener.AdrFam,
						TrAddr:  dnList[vd.VdIdx].DnConf.NvmfListener.TrAddr,
						TrSvcId: dnList[vd.VdIdx].DnConf.NvmfListener.TrSvcId,
					},
					DnSockAddr: vd.DnSockAddr,
					VdIdx:      vd.VdIdx,
					Size:       vd.Size,
				},
				VdFeInfo: &pbds.VdFeInfo{
					ErrInfo: &pbds.ErrInfo{
						IsErr:     true,
						ErrMsg:    lib.ResUninitMsg,
						Timestamp: lib.ResTimestamp(),
					},
				},
			}
			vdFeList = append(vdFeList, vdFe)
		}
		grpFeList := make([]*pbds.GrpFrontend, 0)
		grpFe := &pbds.GrpFrontend{
			GrpId: grp.GrpId,
			GrpFeConf: &pbds.GrpFeConf{
				GrpIdx: grp.GrpIdx,
				Size:   grp.Size,
			},
			GrpFeInfo: &pbds.GrpFeInfo{
				ErrInfo: &pbds.ErrInfo{
					IsErr:     true,
					ErrMsg:    lib.ResUninitMsg,
					Timestamp: lib.ResTimestamp(),
				},
			},
			VdFeList: vdFeList,
		}
		grpFeList = append(grpFeList, grpFe)

		snapFeList := make([]*pbds.SnapFrontend, 0)
		snapFe := &pbds.SnapFrontend{
			SnapId: snap.SnapId,
			SnapFeConf: &pbds.SnapFeConf{
				OriId:   "",
				IsClone: snap.IsClone,
				Idx:     snap.Idx,
				Size:    snap.Size,
			},
			SnapFeInfo: &pbds.SnapFeInfo{
				ErrInfo: &pbds.ErrInfo{
					IsErr:     true,
					ErrMsg:    lib.ResUninitMsg,
					Timestamp: lib.ResTimestamp(),
				},
			},
		}
		snapFeList = append(snapFeList, snapFe)

		expFeList := make([]*pbds.ExpFrontend, 0)

		var primCntlr *pbds.Controller
		cntlrList := make([]*pbds.Controller, 0)
		for i, controllerNode := range cnList {
			isPrimary := false
			if i == 0 {
				isPrimary = true
			}
			cntlr := &pbds.Controller{
				CntlrId:    lib.NewHexStrUuid(),
				CnSockAddr: controllerNode.SockAddr,
				CntlrIdx:   uint32(i),
				IsPrimary:  isPrimary,
				CnNvmfListener: &pbds.NvmfListener{
					TrType:  controllerNode.CnConf.NvmfListener.TrType,
					AdrFam:  controllerNode.CnConf.NvmfListener.AdrFam,
					TrAddr:  controllerNode.CnConf.NvmfListener.TrAddr,
					TrSvcId: controllerNode.CnConf.NvmfListener.TrSvcId,
				},
			}
			cntlrList = append(cntlrList, cntlr)
		}
		diskArray := &pbds.DiskArray{
			DaId:        daId,
			DaName:      req.DaName,
			Description: req.Description,
			DaConf: &pbds.DaConf{
				Qos: &pbds.BdevQos{
					RwIosPerSec:    req.RwIosPerSec,
					RwMbytesPerSec: req.RwMbytesPerSec,
					RMbytesPerSec:  req.RMbytesPerSec,
					WMbytesPerSec:  req.WMbytesPerSec,
				},
				StripCnt:    req.StripCnt,
				StripSizeKb: req.StripSizeKb,
			},
			CntlrList: cntlrList,
			GrpList:   grpList,
			SnapList:  snapList,
			ExpList:   expList,
		}
		daEntityKey := po.kf.DaEntityKey(diskArray.DaName)
		daEntityVal := stm.Get(daEntityKey)
		if len(daEntityVal) != 0 {
			logger.Error("Duplicate DaName: %s", diskArray.DaName)
			msg := fmt.Sprintf("Duplicate DaName: %s", diskArray.DaName)
			return fmt.Errorf(msg)
		}
		newDaEntityVal, err := proto.Marshal(diskArray)
		if err != nil {
			logger.Error("Marshal diskArray err: %v %v", diskArray, err)
			msg := fmt.Sprintf("Marshal diskArray err: %s %v", diskArray.DaName, err)
			return fmt.Errorf(msg)
		}
		stm.Put(daEntityKey, string(newDaEntityVal))

		for i, cand := range dnPdCandList {
			var targetPd *pbds.PhysicalDisk
			diskNode := dnList[i]
			for _, pd := range diskNode.PdList {
				if pd.PdName == cand.PdName {
					targetPd = pd
					break
				}
			}
			if targetPd == nil {
				logger.Warning("Can not find pd from dn: %v %v", cand, diskNode)
				msg := fmt.Sprintf("Can not find pd from dn: %s %s", cand.SockAddr, cand.PdName)
				return retriableError{msg}
			}
			if targetPd.PdInfo.ErrInfo.IsErr {
				logger.Warning("physicalDisk IsErr: %v %v %v", cand, diskNode, targetPd)
				msg := fmt.Sprintf("physicalDisk IsErr: %s %s", cand.SockAddr, cand.PdName)
				return retriableError{msg}
			}
			if targetPd.PdConf.IsOffline {
				logger.Warning("physicalDisk IsOffline: %v %v %v", cand, diskNode, targetPd)
				msg := fmt.Sprintf("physicalDisk IsOffline: %s %s", cand.SockAddr, cand.PdName)
				return retriableError{msg}
			}

			cap := targetPd.Capacity

			oldDnCapKey := po.kf.DnCapKey(cap.FreeSize, diskNode.SockAddr, targetPd.PdName)
			oldDnCapVal := stm.Get(oldDnCapKey)
			if len(oldDnCapVal) == 0 {
				logger.Warning("Can not find dn cap: %s %v", oldDnCapKey, diskNode)
				msg := fmt.Sprintf("Can not find dn cap: %s %s",
					oldDnCapKey, diskNode.SockAddr)
				return fmt.Errorf(msg)
			}
			stm.Del(oldDnCapKey)

			if cap.FreeSize < vdSize {
				logger.Warning("FreeSize not enough: %v %v %v %v",
					cand, diskNode, cap, qos)
				msg := fmt.Sprintf("FreeSize not enough")
				return retriableError{msg}
			} else {
				cap.FreeSize -= vdSize
			}

			if cap.FreeQos.RwIosPerSec != 0 && qos.RwIosPerSec != 0 {
				if cap.FreeQos.RwIosPerSec < qos.RwIosPerSec {
					logger.Warning("RwIosPerSec not enough: %v %v %v %v",
						cand, diskNode, cap, qos)
					msg := fmt.Sprintf("RwIosPerSec not enough")
					return retriableError{msg}
				} else {
					cap.FreeQos.RwIosPerSec -= qos.RwIosPerSec
				}
			} else if cap.FreeQos.RwIosPerSec != 0 && qos.RwIosPerSec == 0 {
				logger.Warning("RwIosPerSec not enough: %v %v %v %v",
					cand, diskNode, cap, qos)
				msg := fmt.Sprintf("RwIosPerSec not enough")
				return retriableError{msg}
			}

			if cap.FreeQos.RwMbytesPerSec != 0 && qos.RwMbytesPerSec != 0 {
				if cap.FreeQos.RwMbytesPerSec < qos.RwMbytesPerSec {
					logger.Warning("RwMbytesPerSec not enough: %v %v %v %v",
						cand, diskNode, cap, qos)
					msg := fmt.Sprintf("RwMbytesPerSec not enough")
					return retriableError{msg}
				} else {
					cap.FreeQos.RwMbytesPerSec -= qos.RwMbytesPerSec
				}
			} else if cap.FreeQos.RwMbytesPerSec != 0 && qos.RwIosPerSec == 0 {
				logger.Warning("RwMbytesPerSec not enough: %v %v %v %v",
					cand, diskNode, cap, qos)
				msg := fmt.Sprintf("RwMbytesPerSec not enough")
				return retriableError{msg}
			}

			if cap.FreeQos.RMbytesPerSec != 0 && qos.RMbytesPerSec != 0 {
				if cap.FreeQos.RMbytesPerSec < qos.RMbytesPerSec {
					logger.Warning("RMbytesPerSec not enough: %v %v %v %v",
						cand, diskNode, cap, qos)
					msg := fmt.Sprintf("RMbytesPerSec not enough")
					return retriableError{msg}
				} else {
					cap.FreeQos.RMbytesPerSec -= qos.RMbytesPerSec
				}
			} else if cap.FreeQos.RMbytesPerSec != 0 && qos.RMbytesPerSec == 0 {
				logger.Warning("RMbytesPerSec not enough: %v %v %v %v",
					cand, diskNode, cap, qos)
				msg := fmt.Sprintf("RMbytesPerSec not enough")
				return retriableError{msg}
			}

			if cap.FreeQos.WMbytesPerSec != 0 && qos.WMbytesPerSec != 0 {
				if cap.FreeQos.WMbytesPerSec < qos.WMbytesPerSec {
					logger.Warning("WMbytesPerSec not enough: %v %v %v %v",
						cand, diskNode, cap, qos)
					msg := fmt.Sprintf("WMbytesPerSec not enough")
					return retriableError{msg}
				} else {
					cap.FreeQos.WMbytesPerSec -= qos.WMbytesPerSec
				}
			} else if cap.FreeQos.WMbytesPerSec != 0 && qos.WMbytesPerSec == 0 {
				logger.Warning("WMbytesPerSec not enough: %v %v %v %v",
					cand, diskNode, cap, qos)
				msg := fmt.Sprintf("WMbytesPerSec not enough")
				return retriableError{msg}
			}

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
			newDnCapVal, err := proto.Marshal(dnSearchAttr)
			if err != nil {
				logger.Error("Marshal dnSearchAttr err: %v %v %v %v",
					diskNode, targetPd, dnSearchAttr, err)
				msg := fmt.Sprintf("Marshal dnSearchAttr err: %v", err)
				return fmt.Errorf(msg)
			}
			stm.Put(newDnCapKey, string(newDnCapVal))

			vd := vdList[i]
			vdBe := &pbds.VdBackend{
				VdId: vd.VdId,
				VdBeConf: &pbds.VdBeConf{
					Size: vd.Size,
					Qos: &pbds.BdevQos{
						RwIosPerSec:    qos.RwIosPerSec,
						RwMbytesPerSec: qos.RwMbytesPerSec,
						RMbytesPerSec:  qos.RMbytesPerSec,
						WMbytesPerSec:  qos.WMbytesPerSec,
					},
					CntlrId: primCntlr.CntlrId,
				},
				VdBeInfo: &pbds.VdBeInfo{
					ErrInfo: &pbds.ErrInfo{
						IsErr:     true,
						ErrMsg:    lib.ResUninitMsg,
						Timestamp: lib.ResTimestamp(),
					},
				},
			}
			targetPd.VdBeList = append(targetPd.VdBeList, vdBe)
			newDnEntityVal, err := proto.Marshal(diskNode)
			if err != nil {
				logger.Error("Marshal diskNode err: %v %v", diskNode, err)
				msg := fmt.Sprintf("Marshal diskNode err: %s %v",
					diskNode.SockAddr, err)
				return fmt.Errorf(msg)
			}
			dnEntityKey := po.kf.DnEntityKey(cand.SockAddr)
			stm.Put(dnEntityKey, string(newDnEntityVal))

			dnErrKey := po.kf.DnErrKey(diskNode.DnConf.HashCode, diskNode.SockAddr)
			dnErrVal := []byte(stm.Get(dnErrKey))
			if len(dnErrVal) == 0 {
				dnSummary := &pbds.DnSummary{
					Description: diskNode.DnConf.Description,
				}
				dnErrVal, err := proto.Marshal(dnSummary)
				if err != nil {
					logger.Error("Marshal dnSummary err: %v %v", dnSummary, err)
					msg := fmt.Sprintf("Marshal dnSummary err: %s %v",
						diskNode.SockAddr, err)
					return fmt.Errorf(msg)
				}
				stm.Put(dnErrKey, string(dnErrVal))
			}
		}

		for i, cand := range cnCandList {
			controllerNode := cnList[i]

			cap := controllerNode.CnCapacity
			oldCnCapKey := po.kf.CnCapKey(cap.CntlrCnt, controllerNode.SockAddr)
			oldCnCapVal := stm.Get(oldCnCapKey)
			if len(oldCnCapVal) == 0 {
				logger.Warning("Can not find cn cap: %s %v", oldCnCapKey, controllerNode)
				msg := fmt.Sprintf("Can not find cn cap: %s %s",
					oldCnCapKey, controllerNode.SockAddr)
				return fmt.Errorf(msg)
			}
			stm.Del(oldCnCapKey)

			cap.CntlrCnt += uint32(1)

			newCnCapKey := po.kf.CnCapKey(cap.CntlrCnt, controllerNode.SockAddr)
			cnSearchAttr := &pbds.CnSearchAttr{
				CnCapacity: &pbds.CnCapacity{
					CntlrCnt: cap.CntlrCnt,
				},
				Location: controllerNode.CnConf.Location,
			}
			newCnCapVal, err := proto.Marshal(cnSearchAttr)
			if err != nil {
				logger.Error("Marshal cnSearchAttr err: %v %v %v",
					controllerNode, cnSearchAttr, err)
				msg := fmt.Sprintf("Marshal cnSearchAttr err: %v", err)
				return fmt.Errorf(msg)
			}
			stm.Put(newCnCapKey, string(newCnCapVal))

			cntlr := cntlrList[i]
			cntlrFe := &pbds.CntlrFrontend{
				CntlrId: cntlr.CntlrId,
				CntlrFeConf: &pbds.CntlrFeConf{
					DaId:        daId,
					DaName:      req.DaName,
					StripSizeKb: req.StripSizeKb,
					CntlrList:   cntlrList,
				},
				CntlrFeInfo: &pbds.CntlrFeInfo{
					ErrInfo: &pbds.ErrInfo{
						IsErr:     true,
						ErrMsg:    lib.ResUninitMsg,
						Timestamp: lib.ResTimestamp(),
					},
				},
				GrpFeList:  grpFeList,
				SnapFeList: snapFeList,
				ExpFeList:  expFeList,
			}
			controllerNode.CntlrFeList = append(controllerNode.CntlrFeList, cntlrFe)
			newCnEntityVal, err := proto.Marshal(controllerNode)
			if err != nil {
				logger.Error("Marshal controllerNode err: %v %v", controllerNode, err)
				msg := fmt.Sprintf("Marshal controllerNode err: %s %v",
					controllerNode.SockAddr, err)
				return fmt.Errorf(msg)
			}
			cnEntityKey := po.kf.CnEntityKey(cand.SockAddr)
			stm.Put(cnEntityKey, string(newCnEntityVal))

			cnErrKey := po.kf.CnErrKey(controllerNode.CnConf.HashCode, controllerNode.SockAddr)
			cnErrVal := []byte(stm.Get(cnErrKey))
			if len(cnErrVal) == 0 {
				cnSummary := &pbds.CnSummary{
					Description: controllerNode.CnConf.Description,
				}
				cnErrVal, err := proto.Marshal(cnSummary)
				if err != nil {
					logger.Error("Marshal cnSummary err: %v %v", cnSummary, err)
					msg := fmt.Sprintf("Marshal cnSummary err: %s %v",
						controllerNode.SockAddr, err)
					return fmt.Errorf(msg)
				}
				stm.Put(cnErrKey, string(cnErrVal))
			}
		}
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "CreateDa: "+req.DaName)
	return err
}

func (po *portalServer) createNewDa(ctx context.Context, req *pbpo.CreateDaRequest) (
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
			logger.Error("Unlock mutex err: %v", err)
		}
	}()

	vdSize := lib.DivCeil(req.PhysicalSize, uint64(req.StripCnt))
	qos := &lib.BdevQos{
		RwIosPerSec:    lib.DivCeil(req.RwIosPerSec, uint64(req.StripCnt)),
		RwMbytesPerSec: lib.DivCeil(req.RwMbytesPerSec, uint64(req.StripCnt)),
		RMbytesPerSec:  lib.DivCeil(req.RMbytesPerSec, uint64(req.StripCnt)),
		WMbytesPerSec:  lib.DivCeil(req.WMbytesPerSec, uint64(req.StripCnt)),
	}

	retryCnt := 0
	for {
		retryCnt++
		if retryCnt > lib.AllocMaxRetry {
			err = fmt.Errorf("Exceed max retry cnt")
			logger.Error("Exceed max retry cnt")
			return dnList, cnList, err
		}
		dnPdCandList, err := po.alloc.AllocDnPd(ctx, req.StripCnt, vdSize, qos)
		if err != nil {
			logger.Error("AllocateDnPd err: %v", err)
			return dnList, cnList, err
		}
		cnCandList, err := po.alloc.AllocCn(ctx, req.CntlrCnt)
		if err != nil {
			logger.Error("AllocateCn err: %v", err)
			return dnList, cnList, err
		}
		err = po.applyAllocation(ctx, req, dnPdCandList, cnCandList, qos, vdSize)
		if err != nil {
			if serr, ok := err.(*retriableError); ok {
				logger.Warning("Retriable error: %v", serr)
				continue
			} else {
				logger.Error("applyAllocation err: %v", err)
				return dnList, cnList, err
			}
		}

		for _, cand := range dnPdCandList {
			dnList = append(dnList, cand.SockAddr)
		}
		for _, cand := range cnCandList {
			cnList = append(cnList, cand.SockAddr)
		}
		return dnList, cnList, nil
	}
}

func (po *portalServer) CreateDa(ctx context.Context, req *pbpo.CreateDaRequest) (
	*pbpo.CreateDaReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DnName is empty"
	} else if req.Size == 0 {
		invalidParamMsg = "Size is zero"
	} else if req.PhysicalSize == 0 {
		invalidParamMsg = "PhysicalSize is zero"
	} else if req.CntlrCnt == 0 {
		invalidParamMsg = "CntlrCnt is zero"
	} else if req.StripCnt == 0 {
		invalidParamMsg = "StripCnt is zero"
	} else if req.StripSizeKb == 0 {
		invalidParamMsg = "StripSizeKb is zero"
	}
	if invalidParamMsg != "" {
		return &pbpo.CreateDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	dnList, cnList, err := po.createNewDa(ctx, req)
	if err != nil {
		return &pbpo.CreateDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}

	for _, sockAddr := range dnList {
		po.sm.SyncupDn(sockAddr, ctx)
	}
	for _, sockAddr := range cnList {
		po.sm.SyncupCn(sockAddr, ctx)
	}
	return &pbpo.CreateDaReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}
