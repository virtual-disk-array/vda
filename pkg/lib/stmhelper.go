package lib

import (
	"fmt"

	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/protobuf/proto"

	"github.com/virtual-disk-array/vda/pkg/logger"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
)

func ChangePrimary(stm concurrency.STM, daName string,
	oldPrimaryId, newPrimaryId string, kf *KeyFmt) error {
	logger.Info("ChangePrimary daName: %s oldPrimaryId: %s\n newPrimaryId: %s",
		daName, oldPrimaryId, newPrimaryId)
	if oldPrimaryId == newPrimaryId {
		return fmt.Errorf("The old and new primary id can not be the same")
	}
	daEntityKey := kf.DaEntityKey(daName)
	diskArray := &pbds.DiskArray{}
	daEntityVal := []byte(stm.Get(daEntityKey))
	if len(daEntityVal) == 0 {
		return fmt.Errorf("Can not find da: %s", daName)
	}
	if err := proto.Unmarshal(daEntityVal, diskArray); err != nil {
		logger.Error("Unmarshal diskArray err: %s %v",
			daEntityKey, err)
		return err
	}

	var oldPrimaryCntlr *pbds.Controller
	var newPrimaryCntlr *pbds.Controller
	for _, cntlr := range diskArray.CntlrList {
		if cntlr.IsPrimary {
			oldPrimaryCntlr = cntlr
			break
		}
	}
	if oldPrimaryCntlr == nil {
		return fmt.Errorf("Can not find primary: %s", daName)
	}
	if oldPrimaryCntlr.CntlrId != oldPrimaryId {
		return fmt.Errorf("oldPrimary has been changed")
	}
	if newPrimaryId == "" {
		for _, cntlr := range diskArray.CntlrList {
			if cntlr.IsPrimary {
				continue
			}
			cnEntityKey := kf.CnEntityKey(cntlr.CnSockAddr)
			cnEntityVal := []byte(stm.Get(cnEntityKey))
			controllerNode := &pbds.ControllerNode{}
			if err := proto.Unmarshal(cnEntityVal, controllerNode); err != nil {
				logger.Error("Unmarshal controllerNode err: %s %v",
					cnEntityKey, err)
				return err
			}
			if controllerNode.CnInfo.ErrInfo.IsErr {
				continue
			}
			if controllerNode.CnConf.IsOffline {
				continue
			}
			newPrimaryCntlr = cntlr
			break
		}
		if newPrimaryCntlr == nil {
			logger.Warning("Can not change primary: %s", daName)
			return nil
		}
	} else {
		for _, cntlr := range diskArray.CntlrList {
			if cntlr.CntlrId == newPrimaryId {
				newPrimaryCntlr = cntlr
				break
			}
		}
		if newPrimaryCntlr == nil {
			return fmt.Errorf("Can not find new primary: %s %s",
				daName, newPrimaryId)
		}
	}
	newPrimaryCntlr.IsPrimary = true
	oldPrimaryCntlr.IsPrimary = false
	logger.Info("newPrimaryCntlr: %v", newPrimaryCntlr)
	logger.Info("oldPrimaryCntlr: %v", oldPrimaryCntlr)
	newDaEntityVal, err := proto.Marshal(diskArray)
	if err != nil {
		logger.Error("Marshal diskArrary err: %v %v",
			diskArray, err)
		return fmt.Errorf("Marshal diskArrary err: %s %v",
			daName, err)
	}
	stm.Put(daEntityKey, string(newDaEntityVal))

	oldCnEntityKey := kf.CnEntityKey(oldPrimaryCntlr.CnSockAddr)
	oldCnEntityVal := []byte(stm.Get(oldCnEntityKey))
	oldCn := &pbds.ControllerNode{}
	if err = proto.Unmarshal(oldCnEntityVal, oldCn); err != nil {
		logger.Error("Unmarshal controllerNode err: %s %v",
			oldCnEntityKey, err)
		return err
	}
	var oldCntlrFe *pbds.CntlrFrontend
	for _, cntlrFe := range oldCn.CntlrFeList {
		if cntlrFe.CntlrId == oldPrimaryCntlr.CntlrId {
			oldCntlrFe = cntlrFe
			break
		}
	}
	if oldCntlrFe == nil {
		return fmt.Errorf("Can not find oldCntlrFe: %v",
			oldPrimaryCntlr)
	}

	newCnEntityKey := kf.CnEntityKey(newPrimaryCntlr.CnSockAddr)
	newCnEntityVal := []byte(stm.Get(newCnEntityKey))
	newCn := &pbds.ControllerNode{}
	if err = proto.Unmarshal(newCnEntityVal, newCn); err != nil {
		logger.Error("Unmarshal controllerNode err: %s %v",
			newCnEntityKey, err)
		return err
	}
	var newCntlrFe *pbds.CntlrFrontend
	for _, cntlrFe := range newCn.CntlrFeList {
		if cntlrFe.CntlrId == newPrimaryCntlr.CntlrId {
			newCntlrFe = cntlrFe
			break
		}
	}
	if newCntlrFe == nil {
		return fmt.Errorf("Can not find newCntlrFe: %v",
			newPrimaryCntlr)
	}

	newCntlrFe.GrpFeList = oldCntlrFe.GrpFeList
	newCntlrFe.SnapFeList = oldCntlrFe.SnapFeList
	newCntlrFe.MtFeList = oldCntlrFe.MtFeList
	newCntlrFe.ItFeList = oldCntlrFe.ItFeList
	newCntlrFe.IsInited = oldCntlrFe.IsInited
	for _, cntlr := range newCntlrFe.CntlrFeConf.CntlrList {
		if cntlr.CntlrId == newPrimaryCntlr.CntlrId {
			cntlr.IsPrimary = true
		} else {
			cntlr.IsPrimary = false
		}
	}

	oldCntlrFe.GrpFeList = make([]*pbds.GrpFrontend, 0)
	oldCntlrFe.SnapFeList = make([]*pbds.SnapFrontend, 0)
	oldCntlrFe.MtFeList = make([]*pbds.MtFrontend, 0)
	oldCntlrFe.ItFeList = make([]*pbds.ItFrontend, 0)
	oldCntlrFe.IsInited = false
	for _, cntlr := range oldCntlrFe.CntlrFeConf.CntlrList {
		if cntlr.CntlrId == newPrimaryCntlr.CntlrId {
			cntlr.IsPrimary = true
		} else {
			cntlr.IsPrimary = false
		}
	}

	oldCn.Version++
	oldCnEntityVal, err = proto.Marshal(oldCn)
	if err != nil {
		logger.Error("Marshal oldCn err: %v %v",
			oldCn, err)
		return err
	}
	stm.Put(oldCnEntityKey, string(oldCnEntityVal))
	oldCnErrKey := kf.CnErrKey(oldCn.CnConf.HashCode, oldCn.SockAddr)
	if len(stm.Get(oldCnErrKey)) == 0 {
		oldCnSummary := &pbds.CnSummary{
			SockAddr: oldCn.SockAddr,
			Description: oldCn.CnConf.Description,
		}
		oldCnErrVal, err := proto.Marshal(oldCnSummary)
		if err != nil {
			logger.Error("Marshal oldCnSummary err: %v %v",
				oldCnSummary, err)
			return err
		}
		stm.Put(oldCnErrKey, string(oldCnErrVal))
	}

	newCn.Version++
	newCnEntityVal, err = proto.Marshal(newCn)
	if err != nil {
		logger.Error("Marshal newCn err: %v %v",
			newCn, err)
		return err
	}
	stm.Put(newCnEntityKey, string(newCnEntityVal))
	newCnErrKey := kf.CnErrKey(newCn.CnConf.HashCode, newCn.SockAddr)
	if len(stm.Get(newCnErrKey)) == 0 {
		newCnSummary := &pbds.CnSummary{
			SockAddr: newCn.SockAddr,
			Description: newCn.CnConf.Description,
		}
		newCnErrVal, err := proto.Marshal(newCnSummary)
		if err != nil {
			logger.Error("Marshal newCnSummary err: %v %v",
				newCnSummary, err)
			return err
		}
		stm.Put(newCnErrKey, string(newCnErrVal))
	}

	for _, grp := range diskArray.GrpList {
		for _, vd := range grp.VdList {
			diskNode := &pbds.DiskNode{}
			dnEntityKey := kf.DnEntityKey(vd.DnSockAddr)
			dnEntityVal := []byte(stm.Get(dnEntityKey))
			if len(dnEntityVal) == 0 {
				logger.Error("Can not find diskNode: %s",
					vd.DnSockAddr)
				return fmt.Errorf("Can not find diskNode: %s",
					vd.DnSockAddr)
			}
			if err := proto.Unmarshal(dnEntityVal, diskNode); err != nil {
				logger.Error("Unmarshal diskNode err: %s %v",
					vd.DnSockAddr, err)
				return err
			}
			var thisPd *pbds.PhysicalDisk
			for _, pd := range diskNode.PdList {
				if pd.PdName == vd.PdName {
					thisPd = pd
					break
				}
			}
			if thisPd == nil {
				logger.Error("Can not find pd: %s %v",
					vd.PdName, diskNode)
				return fmt.Errorf("Can not find pd: %s %s",
					vd.PdName, vd.DnSockAddr)
			}
			var thisVdBe *pbds.VdBackend
			for _, vdBe := range thisPd.VdBeList {
				if vdBe.VdId == vd.VdId {
					thisVdBe = vdBe
					break
				}
			}
			if thisVdBe == nil {
				logger.Error("Can not find vdBe: %s %s %v",
					vd.VdId, vd.PdName, diskNode)
				return fmt.Errorf("Can not find vdBe: %s %s %s",
					vd.VdId, vd.PdName, vd.DnSockAddr)
			}
			thisVdBe.VdBeConf.CntlrId = newPrimaryCntlr.CntlrId
			diskNode.Version++
			newDnEntityVal, err := proto.Marshal(diskNode)
			if err != nil {
				logger.Error("Marshal diskNode err: %v %v",
					diskNode, err)
				return err
			}
			stm.Put(dnEntityKey, string(newDnEntityVal))
			dnErrKey := kf.DnErrKey(diskNode.DnConf.HashCode,
				diskNode.SockAddr)
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
		}
	}
	return nil
}
