package portal

import (
	"context"
	"encoding/base64"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/protobuf/proto"

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
	grpSize := vdSize * uint64(req.DaConf.StripCnt)

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
		emptyGrpFeList := make([]*pbds.GrpFrontend, 0)
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
		emptySnapFeList := make([]*pbds.SnapFrontend, 0)
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
			if isPrimary {
				primCntlr = cntlr
			}
		}
		diskArray := &pbds.DiskArray{
			DaId:        daId,
			DaName:      req.DaName,
			Description: req.Description,
			DaConf: &pbds.DaConf{
				Qos: &pbds.BdevQos{
					RwIosPerSec:    req.DaConf.Qos.RwIosPerSec,
					RwMbytesPerSec: req.DaConf.Qos.RwMbytesPerSec,
					RMbytesPerSec:  req.DaConf.Qos.RMbytesPerSec,
					WMbytesPerSec:  req.DaConf.Qos.WMbytesPerSec,
				},
				StripCnt:    req.DaConf.StripCnt,
				StripSizeKb: req.DaConf.StripSizeKb,
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
			return &portalError{
				code: lib.PortalDupResErrCode,
				msg:  daEntityKey,
			}
		}
		newDaEntityVal, err := proto.Marshal(diskArray)
		if err != nil {
			logger.Error("Marshal diskArray err: %v %v", diskArray, err)
			return fmt.Errorf("Marshal diskArray err: %s %v", diskArray.DaName, err)
		}
		stm.Put(daEntityKey, string(newDaEntityVal))

		daSummary := &pbds.DaSummary{
			DaName:      req.DaName,
			Description: req.Description,
		}
		daListKey := po.kf.DaListKey(req.DaName)
		daListVal, err := proto.Marshal(daSummary)
		if err != nil {
			logger.Error("Marshal daSummary err: %v %v", daSummary, err)
			return fmt.Errorf("Marshal daSummary err: %s %v", diskArray.DaName, err)
		}
		stm.Put(daListKey, string(daListVal))

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
				return fmt.Errorf("Can not find dn cap: %s %s",
					oldDnCapKey, diskNode.SockAddr)
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
				return fmt.Errorf("Marshal dnSearchAttr err: %v", err)
			}
			stm.Put(newDnCapKey, string(newDnCapVal))

			vd := vdList[i]
			vdBe := &pbds.VdBackend{
				VdId: vd.VdId,
				VdBeConf: &pbds.VdBeConf{
					DaName: req.DaName,
					GrpIdx: uint32(0),
					VdIdx:  uint32(i),
					Size:   vd.Size,
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
			diskNode.Version++
			newDnEntityVal, err := proto.Marshal(diskNode)
			if err != nil {
				logger.Error("Marshal diskNode err: %v %v", diskNode, err)
				return fmt.Errorf("Marshal diskNode err: %s %v",
					diskNode.SockAddr, err)
			}
			dnEntityKey := po.kf.DnEntityKey(cand.SockAddr)
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
		}

		for i, cand := range cnCandList {
			controllerNode := cnList[i]

			cap := controllerNode.CnCapacity
			oldCnCapKey := po.kf.CnCapKey(cap.CntlrCnt, controllerNode.SockAddr)
			oldCnCapVal := stm.Get(oldCnCapKey)
			if len(oldCnCapVal) == 0 {
				logger.Warning("Can not find cn cap: %s %v", oldCnCapKey, controllerNode)
				return fmt.Errorf("Can not find cn cap: %s %s",
					oldCnCapKey, controllerNode.SockAddr)
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
				return fmt.Errorf("Marshal cnSearchAttr err: %v", err)
			}
			stm.Put(newCnCapKey, string(newCnCapVal))

			cntlr := cntlrList[i]
			var thisGrpFeList []*pbds.GrpFrontend
			var thisSnapFeList []*pbds.SnapFrontend
			if cntlr.IsPrimary {
				thisGrpFeList = grpFeList
				thisSnapFeList = snapFeList
			} else {
				thisGrpFeList = emptyGrpFeList
				thisSnapFeList = emptySnapFeList
			}
			cntlrFe := &pbds.CntlrFrontend{
				CntlrId: cntlr.CntlrId,
				CntlrFeConf: &pbds.CntlrFeConf{
					DaId:        daId,
					DaName:      req.DaName,
					StripSizeKb: req.DaConf.StripSizeKb,
					CntlrList:   cntlrList,
				},
				CntlrFeInfo: &pbds.CntlrFeInfo{
					ErrInfo: &pbds.ErrInfo{
						IsErr:     true,
						ErrMsg:    lib.ResUninitMsg,
						Timestamp: lib.ResTimestamp(),
					},
				},
				IsInit:     false,
				GrpFeList:  thisGrpFeList,
				SnapFeList: thisSnapFeList,
				ExpFeList:  expFeList,
			}
			controllerNode.CntlrFeList = append(controllerNode.CntlrFeList, cntlrFe)
			controllerNode.Version++
			newCnEntityVal, err := proto.Marshal(controllerNode)
			if err != nil {
				logger.Error("Marshal controllerNode err: %v %v", controllerNode, err)
				return fmt.Errorf("Marshal controllerNode err: %s %v",
					controllerNode.SockAddr, err)
			}
			cnEntityKey := po.kf.CnEntityKey(cand.SockAddr)
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

	vdSize := lib.DivCeil(req.PhysicalSize, uint64(req.DaConf.StripCnt))
	qos := &lib.BdevQos{
		RwIosPerSec:    lib.DivCeil(req.DaConf.Qos.RwIosPerSec, uint64(req.DaConf.StripCnt)),
		RwMbytesPerSec: lib.DivCeil(req.DaConf.Qos.RwMbytesPerSec, uint64(req.DaConf.StripCnt)),
		RMbytesPerSec:  lib.DivCeil(req.DaConf.Qos.RMbytesPerSec, uint64(req.DaConf.StripCnt)),
		WMbytesPerSec:  lib.DivCeil(req.DaConf.Qos.WMbytesPerSec, uint64(req.DaConf.StripCnt)),
	}

	retryCnt := 0
	for {
		retryCnt++
		if retryCnt > lib.AllocMaxRetry {
			err = fmt.Errorf("Exceed max retry cnt")
			logger.Error("Exceed max retry cnt")
			return dnList, cnList, err
		}
		dnPdCandList, err := po.alloc.AllocDnPd(ctx, req.DaConf.StripCnt, vdSize, qos)
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
	} else if req.DaConf == nil {
		invalidParamMsg = "DaConf is empty"
	} else if req.DaConf.Qos == nil {
		invalidParamMsg = "Qos is empty"
	} else if req.DaConf.StripCnt == 0 {
		invalidParamMsg = "StripCnt is zero"
	} else if req.DaConf.StripSizeKb == 0 {
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
		if serr, ok := err.(*portalError); ok {
			return &pbpo.CreateDaReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.CreateDaReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
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
	return &pbpo.CreateDaReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}

func (po *portalServer) DeleteDa(ctx context.Context, req *pbpo.DeleteDaRequest) (
	*pbpo.DeleteDaReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DnName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.DeleteDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	daEntityKey := po.kf.DaEntityKey(req.DaName)
	daListKey := po.kf.DaListKey(req.DaName)
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
			logger.Error("Unmarshal diskArray err: %s %v", daEntityKey, err)
			return err
		}
		if len(diskArray.ExpList) > 0 {
			return &portalError{
				lib.PortalResBusyErrCode,
				"diskArray has exporter(s)",
			}
		}
		stm.Del(daEntityKey)
		stm.Del(daListKey)

		for _, cntlr := range diskArray.CntlrList {
			cnEntityKey := po.kf.CnEntityKey(cntlr.CnSockAddr)
			cnEntityVal := []byte(stm.Get(cnEntityKey))
			if len(cnEntityVal) == 0 {
				logger.Error("Can not find cn: %s", cnEntityKey)
				return &portalError{
					lib.PortalInternalErrCode,
					fmt.Sprintf("Can not find cn: %s", cntlr.CnSockAddr),
				}
			}
			controllerNode := &pbds.ControllerNode{}
			if err := proto.Unmarshal(cnEntityVal, controllerNode); err != nil {
				logger.Error("Unmarshal controllerNode err: %s %v", cnEntityKey, err)
				return err
			}
			targetIdx := -1
			for i, cntlrFe := range controllerNode.CntlrFeList {
				if cntlrFe.CntlrId == cntlr.CntlrId {
					targetIdx = i
				}
			}
			if targetIdx == -1 {
				logger.Error("Can not find cntlr: %v %v", controllerNode, cntlr)
				return &portalError{
					lib.PortalInternalErrCode,
					"Can not find cntlr",
				}
			}
			length := len(controllerNode.CntlrFeList)
			controllerNode.CntlrFeList[targetIdx] = controllerNode.CntlrFeList[length-1]
			controllerNode.CntlrFeList = controllerNode.CntlrFeList[:length-1]
			cap := controllerNode.CnCapacity
			if cap.CntlrCnt == 0 {
				logger.Error("Invalid CnCapacity: %v", controllerNode)
				return &portalError{
					lib.PortalInternalErrCode,
					"Invalid CnCapacity",
				}
			}
			oldCnCapKey := po.kf.CnCapKey(cap.CntlrCnt, controllerNode.SockAddr)
			cap.CntlrCnt -= 1
			newCnCapKey := po.kf.CnCapKey(cap.CntlrCnt, controllerNode.SockAddr)
			cnSearchAttr := &pbds.CnSearchAttr{
				CnCapacity: cap,
				Location:   controllerNode.CnConf.Location,
			}
			cnCapVal, err := proto.Marshal(cnSearchAttr)
			if err != nil {
				logger.Error("Marshal cnSearchAttr err: %v %v", cnSearchAttr, err)
				return err
			}
			stm.Del(oldCnCapKey)
			stm.Put(newCnCapKey, string(cnCapVal))

			controllerNode.Version++
			newCnEntityVal, err := proto.Marshal(controllerNode)
			if err != nil {
				logger.Error("Marshal controllerNode err: %v %v", controllerNode, err)
				return err
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
					return err
				}
				stm.Put(cnErrKey, string(cnErrVal))
			}
		}

		for _, grp := range diskArray.GrpList {
			for _, vd := range grp.VdList {
				dnEntityKey := po.kf.DnEntityKey(vd.DnSockAddr)
				dnEntityVal := []byte(stm.Get(dnEntityKey))
				if len(dnEntityVal) == 0 {
					logger.Error("Can not find dn: %s", dnEntityKey)
					return &portalError{
						lib.PortalInternalErrCode,
						fmt.Sprintf("Can not find dn: %s", vd.DnSockAddr),
					}
				}
				diskNode := &pbds.DiskNode{}
				if err := proto.Unmarshal(dnEntityVal, diskNode); err != nil {
					logger.Error("Unmarshal diskNode err: %s %v", dnEntityVal, err)
					return err
				}
				var targetPd *pbds.PhysicalDisk
				for _, pd := range diskNode.PdList {
					if pd.PdName == vd.PdName {
						targetPd = pd
						break
					}
				}
				if targetPd == nil {
					logger.Error("Can not find pd: %v %v", diskNode, vd)
					return &portalError{
						lib.PortalInternalErrCode,
						"Can not find pd",
					}
				}
				targetIdx := -1
				for i, vdBe := range targetPd.VdBeList {
					if vdBe.VdId == vd.VdId {
						targetIdx = i
						break
					}
				}
				if targetIdx == -1 {
					logger.Error("Can not find vd: %v %v", diskNode, vd)
					return &portalError{
						lib.PortalInternalErrCode,
						"Can not find vd",
					}
				}
				length := len(targetPd.VdBeList)
				targetPd.VdBeList[targetIdx] = targetPd.VdBeList[length-1]
				targetPd.VdBeList = targetPd.VdBeList[:length-1]

				cap := targetPd.Capacity
				oldDnCapKey := po.kf.DnCapKey(cap.FreeSize, diskNode.SockAddr, targetPd.PdName)
				cap.FreeSize += vd.Size
				cap.FreeQos.RwIosPerSec += vd.Qos.RwIosPerSec
				cap.FreeQos.RwMbytesPerSec += vd.Qos.RwMbytesPerSec
				cap.FreeQos.RMbytesPerSec += vd.Qos.RMbytesPerSec
				cap.FreeQos.WMbytesPerSec += vd.Qos.WMbytesPerSec
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
					logger.Error("marshal dnSearchAttr err: %v %v", dnSearchAttr, err)
					return err
				}
				stm.Del(oldDnCapKey)
				stm.Put(newDnCapKey, string(dnCapVal))
				diskNode.Version++
				newDnEntityVal, err := proto.Marshal(diskNode)
				if err != nil {
					logger.Error("Marshal diskNode err: %s %v %v", dnEntityKey, diskNode, err)
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
						logger.Error("Marshal dnSummary err: %v %v", dnSummary, err)
						return err
					}
					stm.Put(dnErrKey, string(dnErrVal))
				}
			}
		}
		return nil
	}

	session, err := concurrency.NewSession(po.etcdCli,
		concurrency.WithTTL(lib.AllocLockTTL))
	if err != nil {
		logger.Error("Create session err: %v", err)
		return &pbpo.DeleteDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	defer session.Close()
	mutex := concurrency.NewMutex(session, po.kf.AllocLockPath())
	if err = mutex.Lock(ctx); err != nil {
		logger.Error("Lock mutex err: %v", err)
		return &pbpo.DeleteDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	defer func() {
		if err := mutex.Unlock(ctx); err != nil {
			logger.Error("Unlock mutex err: %v", err)
		}
	}()

	err = po.sw.RunStm(apply, ctx, "DeleteDa: "+req.DaName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.DeleteDaReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.DeleteDaReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.DeleteDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
		}, nil
	}
}

func (po *portalServer) modifyDaDescription(ctx context.Context, daName string,
	description string) (*pbpo.ModifyDaReply, error) {
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
			logger.Error("Unmarshal diskArray err: %s %v", daEntityKey, err)
			return err
		}
		diskArray.Description = description
		newDaEntityVal, err := proto.Marshal(diskArray)
		if err != nil {
			logger.Error("Marshal diskArray err: %v %v", diskArray, err)
			return err
		}
		stm.Put(daEntityKey, string(newDaEntityVal))
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "ModifyDaDescription: "+daName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.ModifyDaReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.ModifyDaReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.ModifyDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
		}, nil
	}
}

func (po *portalServer) ModifyDa(ctx context.Context, req *pbpo.ModifyDaRequest) (
	*pbpo.ModifyDaReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.ModifyDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	switch x := req.Attr.(type) {
	case *pbpo.ModifyDaRequest_Description:
		return po.modifyDaDescription(ctx, req.DaName, x.Description)
	default:
		logger.Error("Unknow attr: %v", x)
		return &pbpo.ModifyDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  "Unknow attr",
			},
		}, nil
	}
}

func (po *portalServer) listDaWithoutToken(ctx context.Context, limit int64) (
	*pbpo.ListDaReply, error) {
	opts := []clientv3.OpOption{
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(limit),
		clientv3.WithPrefix(),
	}
	kv := clientv3.NewKV(po.etcdCli)
	prefix := po.kf.DaListPrefix()
	gr, err := kv.Get(ctx, prefix, opts...)
	if err != nil {
		return &pbpo.ListDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	daSummaryList := make([]*pbpo.DaSummary, 0)
	for _, item := range gr.Kvs {
		daName, err := po.kf.DecodeDaListKey(string(item.Key))
		if err != nil {
			return &pbpo.ListDaReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
		dsDaSummary := &pbds.DaSummary{}
		if err := proto.Unmarshal(item.Value, dsDaSummary); err != nil {
			return &pbpo.ListDaReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
		poDaSummary := &pbpo.DaSummary{
			DaName:      daName,
			Description: dsDaSummary.Description,
		}
		daSummaryList = append(daSummaryList, poDaSummary)
	}
	token := ""
	if len(gr.Kvs) > 0 {
		lastKey := gr.Kvs[len(gr.Kvs)-1].Key
		token = base64.StdEncoding.EncodeToString(lastKey)
	}
	return &pbpo.ListDaReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
		Token:         token,
		DaSummaryList: daSummaryList,
	}, nil
}

func (po *portalServer) listDaWithToken(ctx context.Context, limit int64,
	token string) (*pbpo.ListDaReply, error) {
	opts := []clientv3.OpOption{
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(limit + 1),
		clientv3.WithFromKey(),
	}
	kv := clientv3.NewKV(po.etcdCli)
	lastKey, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return &pbpo.ListDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	gr, err := kv.Get(ctx, string(lastKey), opts...)
	if err != nil {
		return &pbpo.ListDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	daSummaryList := make([]*pbpo.DaSummary, 0)
	if len(gr.Kvs) <= 1 {
		return &pbpo.ListDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
			Token:         "",
			DaSummaryList: daSummaryList,
		}, nil
	}
	for _, item := range gr.Kvs[1:] {
		daName, err := po.kf.DecodeDaListKey(string(item.Key))
		if err != nil {
			if serr, ok := err.(*lib.InvalidKeyError); ok {
				logger.Info("listDaWithToken InvalidKeyError: %v", serr)
				break
			} else {
				return &pbpo.ListDaReply{
					ReplyInfo: &pbpo.ReplyInfo{
						ReqId:     lib.GetReqId(ctx),
						ReplyCode: lib.PortalInternalErrCode,
						ReplyMsg:  err.Error(),
					},
				}, nil
			}
		}
		dsDaSummary := &pbds.DaSummary{}
		if err := proto.Unmarshal(item.Value, dsDaSummary); err != nil {
			return &pbpo.ListDaReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
		if dsDaSummary.DaName != daName {
			logger.Error("daName mismatch: %v %s", dsDaSummary, daName)
			return &pbpo.ListDaReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  "daName mismatch",
				},
			}, nil
		}
		poDaSummary := &pbpo.DaSummary{
			DaName:      dsDaSummary.DaName,
			Description: dsDaSummary.Description,
		}
		daSummaryList = append(daSummaryList, poDaSummary)
	}
	nextToken := base64.StdEncoding.EncodeToString(gr.Kvs[len(gr.Kvs)-1].Key)
	return &pbpo.ListDaReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
		Token:         nextToken,
		DaSummaryList: daSummaryList,
	}, nil
}

func (po *portalServer) ListDa(ctx context.Context, req *pbpo.ListDaRequest) (
	*pbpo.ListDaReply, error) {
	limit := lib.PortalDefaultListLimit
	if req.Limit > lib.PortalMaxListLimit {
		invalidParamMsg := fmt.Sprintf("Limit is larger than %d",
			lib.PortalMaxListLimit)
		return &pbpo.ListDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	} else if req.Limit != 0 {
		limit = req.Limit
	}
	if req.Token == "" {
		return po.listDaWithoutToken(ctx, limit)
	} else {
		return po.listDaWithToken(ctx, limit, req.Token)
	}
}

func (po *portalServer) GetDa(ctx context.Context, req *pbpo.GetDaRequest) (
	*pbpo.GetDaReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.GetDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	daEntityKey := po.kf.DaEntityKey(req.DaName)
	diskArray := &pbds.DiskArray{}
	addrToCn := make(map[string]*pbds.ControllerNode)
	addrToDn := make(map[string]*pbds.DiskNode)

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

		for _, cntlr := range diskArray.CntlrList {
			cnEntityKey := po.kf.CnEntityKey(cntlr.CnSockAddr)
			cnEntityVal := []byte(stm.Get(cnEntityKey))
			controllerNode := &pbds.ControllerNode{}
			if err := proto.Unmarshal(cnEntityVal, controllerNode); err != nil {
				logger.Error("Unmarshal controllerNode err: %s %v", cnEntityKey, err)
				return err
			}
			addrToCn[cntlr.CnSockAddr] = controllerNode
		}
		for _, grp := range diskArray.GrpList {
			for _, vd := range grp.VdList {
				dnEntityKey := po.kf.DnEntityKey(vd.DnSockAddr)
				dnEntityVal := []byte(stm.Get(dnEntityKey))
				diskNode := &pbds.DiskNode{}
				if err := proto.Unmarshal(dnEntityVal, diskNode); err != nil {
					logger.Error("Unmarshal diskNode err: %s %v", dnEntityKey, err)
					return err
				}
				addrToDn[vd.DnSockAddr] = diskNode
			}
		}
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "GetDa: "+req.DaName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.GetDaReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.GetDaReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		cntlrList := make([]*pbpo.Controller, 0)
		idToGrpFeInfo := make(map[string]*pbds.GrpFeInfo)
		idToVdFeInfo := make(map[string]*pbds.VdFeInfo)
		for _, dsCntlr := range diskArray.CntlrList {
			controllerNode, ok := addrToCn[dsCntlr.CnSockAddr]
			if !ok {
				return &pbpo.GetDaReply{
					ReplyInfo: &pbpo.ReplyInfo{
						ReqId:     lib.GetReqId(ctx),
						ReplyCode: lib.PortalInternalErrCode,
						ReplyMsg:  "No controllerNode: " + dsCntlr.CnSockAddr,
					},
				}, nil
			}
			var dsCntlrFeInfo *pbds.CntlrFeInfo
			for _, cntlrFe := range controllerNode.CntlrFeList {
				if cntlrFe.CntlrId == dsCntlr.CntlrId {
					dsCntlrFeInfo = cntlrFe.CntlrFeInfo
					for _, grpFe := range cntlrFe.GrpFeList {
						idToGrpFeInfo[grpFe.GrpId] = grpFe.GrpFeInfo
						for _, vdFe := range grpFe.VdFeList {
							idToVdFeInfo[vdFe.VdId] = vdFe.VdFeInfo
						}
					}
					break
				}
			}
			if dsCntlrFeInfo == nil {
				return &pbpo.GetDaReply{
					ReplyInfo: &pbpo.ReplyInfo{
						ReqId:     lib.GetReqId(ctx),
						ReplyCode: lib.PortalInternalErrCode,
						ReplyMsg:  "No cntlrInfo: " + dsCntlr.CntlrId,
					},
				}, nil
			}
			poCntlr := &pbpo.Controller{
				CntlrId:   dsCntlr.CntlrId,
				SockAddr:  dsCntlr.CnSockAddr,
				CntlrIdx:  dsCntlr.CntlrIdx,
				IsPrimary: dsCntlr.IsPrimary,
				ErrInfo: &pbpo.ErrInfo{
					IsErr:     dsCntlrFeInfo.ErrInfo.IsErr,
					ErrMsg:    dsCntlrFeInfo.ErrInfo.ErrMsg,
					Timestamp: dsCntlrFeInfo.ErrInfo.Timestamp,
				},
			}
			cntlrList = append(cntlrList, poCntlr)
		}
		grpList := make([]*pbpo.Group, 0)
		for _, dsGrp := range diskArray.GrpList {
			vdList := make([]*pbpo.VirtualDisk, 0)
			for _, dsVd := range dsGrp.VdList {
				dsVdFeInfo, ok := idToVdFeInfo[dsVd.VdId]
				if !ok {
					return &pbpo.GetDaReply{
						ReplyInfo: &pbpo.ReplyInfo{
							ReqId:     lib.GetReqId(ctx),
							ReplyCode: lib.PortalInternalErrCode,
							ReplyMsg:  "No vdFeInfo: " + dsVd.VdId,
						},
					}, nil
				}
				diskNode, ok := addrToDn[dsVd.DnSockAddr]
				if !ok {
					return &pbpo.GetDaReply{
						ReplyInfo: &pbpo.ReplyInfo{
							ReqId:     lib.GetReqId(ctx),
							ReplyCode: lib.PortalInternalErrCode,
							ReplyMsg:  "No diskNode: " + dsVd.DnSockAddr,
						},
					}, nil
				}
				var dsVdBeInfo *pbds.VdBeInfo
				for _, pd := range diskNode.PdList {
					if pd.PdName == dsVd.PdName {
						for _, vdBe := range pd.VdBeList {
							if vdBe.VdId == dsVd.VdId {
								dsVdBeInfo = vdBe.VdBeInfo
								break
							}
						}
					}
				}
				if dsVdBeInfo == nil {
					return &pbpo.GetDaReply{
						ReplyInfo: &pbpo.ReplyInfo{
							ReqId:     lib.GetReqId(ctx),
							ReplyCode: lib.PortalInternalErrCode,
							ReplyMsg:  "No dsVdBeInfo: " + dsVd.VdId,
						},
					}, nil
				}
				poVd := &pbpo.VirtualDisk{
					VdId:     dsVd.VdId,
					VdIdx:    dsVd.VdIdx,
					SockAddr: dsVd.DnSockAddr,
					PdName:   dsVd.PdName,
					Size:     dsVd.Size,
					Qos: &pbpo.BdevQos{
						RwIosPerSec:    dsVd.Qos.RwIosPerSec,
						RwMbytesPerSec: dsVd.Qos.RwMbytesPerSec,
						RMbytesPerSec:  dsVd.Qos.RMbytesPerSec,
						WMbytesPerSec:  dsVd.Qos.WMbytesPerSec,
					},
					BeErrInfo: &pbpo.ErrInfo{
						IsErr:     dsVdBeInfo.ErrInfo.IsErr,
						ErrMsg:    dsVdBeInfo.ErrInfo.ErrMsg,
						Timestamp: dsVdBeInfo.ErrInfo.Timestamp,
					},
					FeErrInfo: &pbpo.ErrInfo{
						IsErr:     dsVdFeInfo.ErrInfo.IsErr,
						ErrMsg:    dsVdFeInfo.ErrInfo.ErrMsg,
						Timestamp: dsVdFeInfo.ErrInfo.Timestamp,
					},
				}
				vdList = append(vdList, poVd)
			}
			dsGrpFeInfo, ok := idToGrpFeInfo[dsGrp.GrpId]
			if !ok {
				return &pbpo.GetDaReply{
					ReplyInfo: &pbpo.ReplyInfo{
						ReqId:     lib.GetReqId(ctx),
						ReplyCode: lib.PortalInternalErrCode,
						ReplyMsg:  "No grpFeInfo: " + dsGrp.GrpId,
					},
				}, nil
			}
			poGrp := &pbpo.Group{
				GrpId:  dsGrp.GrpId,
				GrpIdx: dsGrp.GrpIdx,
				Size:   dsGrp.Size,
				ErrInfo: &pbpo.ErrInfo{
					IsErr:     dsGrpFeInfo.ErrInfo.IsErr,
					ErrMsg:    dsGrpFeInfo.ErrInfo.ErrMsg,
					Timestamp: dsGrpFeInfo.ErrInfo.Timestamp,
				},
				VdList: vdList,
			}
			grpList = append(grpList, poGrp)
		}
		return &pbpo.GetDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
			DiskArray: &pbpo.DiskArray{
				DaId:        diskArray.DaId,
				DaName:      diskArray.DaName,
				Description: diskArray.Description,
				DaConf: &pbpo.DaConf{
					Qos: &pbpo.BdevQos{
						RwIosPerSec:    diskArray.DaConf.Qos.RwIosPerSec,
						RwMbytesPerSec: diskArray.DaConf.Qos.RwMbytesPerSec,
						RMbytesPerSec:  diskArray.DaConf.Qos.RMbytesPerSec,
						WMbytesPerSec:  diskArray.DaConf.Qos.WMbytesPerSec,
					},
					StripCnt:    diskArray.DaConf.StripCnt,
					StripSizeKb: diskArray.DaConf.StripSizeKb,
				},
				CntlrList: cntlrList,
				GrpList:   grpList,
			},
		}, nil
	}
}
