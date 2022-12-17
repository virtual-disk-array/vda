package portal

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/protobuf/proto"
	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

func (po *portalServer) CreateSnap(ctx context.Context, req *pbpo.CreateSnapRequest) (*pbpo.CreateSnapReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	} else if req.SnapName == "" {
		invalidParamMsg = "SnapName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.CreateSnapReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	daEntityKey := po.kf.DaEntityKey(req.DaName)
	diskArray := &pbds.DiskArray{}

	var primarySockAddr string
	var newSnap *pbds.Snap

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
		oriName := ""
		oriId := ""
		oriSize := uint64(0)
		if req.OriName == "" {
			oriSize = diskArray.DaConf.Size
		}
		if req.OriName != "" {
			for _, snap := range diskArray.SnapList {
				if snap.SnapName == req.OriName {
					oriName = snap.SnapName
					oriId = snap.SnapId
					oriSize = snap.Size
					break
				}
			}
			if oriName == "" {
				return fmt.Errorf("OriSnap does not exist: %v", req.OriName)
			}
		}
		if oriSize == 0 {
			panic("oriSize is zero, it should never happen")
		}
		var newSnapSize uint64
		if req.Size == 0 {
			newSnapSize = oriSize
		} else {
			newSnapSize = req.Size
		}

		if newSnapSize < oriSize {
			return fmt.Errorf("request parameter err: req.Size should greater than ori size %d", oriSize)
		}

		newSnap = &pbds.Snap{
			SnapId:      lib.NewHexStrUuid(),
			SnapName:    req.SnapName,
			Description: req.Description,
			OriName:     oriName,
			Idx:         uint64(stm.Rev(daEntityKey)),
			Size:        newSnapSize,
		}

		diskArray.SnapList = append(diskArray.SnapList, newSnap)

		newDaEntityVal, err := proto.Marshal(diskArray)
		if err != nil {
			logger.Error("Marshal diskArray err: %v %v", diskArray, err)
			return fmt.Errorf("marshal diskArray err: %v", err)
		}

		stm.Put(daEntityKey, string(newDaEntityVal))

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
			logger.Error("Unmarshal controllerNode err: %s %v", cnEntityKey, err)
			return err
		}
		primarySockAddr = controllerNode.SockAddr
		snapFe := &pbds.SnapFrontend{
			SnapId: newSnap.SnapId,
			SnapFeConf: &pbds.SnapFeConf{
				OriId:   oriId,
				Idx:     newSnap.Idx,
				Size:    newSnap.Size,
			},
			SnapFeInfo: &pbds.SnapFeInfo{
				ErrInfo: &pbds.ErrInfo{
					IsErr:     true,
					ErrMsg:    lib.ResUninitMsg,
					Timestamp: lib.ResTimestamp(),
				},
			},
		}
		var targetCntlrFe *pbds.CntlrFrontend
		for _, cntlrFe := range controllerNode.CntlrFeList {
			if cntlrFe.CntlrId == targetCntlr.CntlrId {
				targetCntlrFe = cntlrFe
				break
			}
		}
		if targetCntlrFe == nil {
			logger.Error("Can not find cntlr: %v %v", targetCntlr, controllerNode)
			return fmt.Errorf("Can not find cntlr")
		}
		targetCntlrFe.SnapFeList = append(targetCntlrFe.SnapFeList, snapFe)
		controllerNode.Version++
		newCnEntityVal, err := proto.Marshal(controllerNode)
		if err != nil {
			logger.Error("Marshal controllerNode err: %v %v", controllerNode, err)
			return fmt.Errorf("marshal controllerNode err: %v", err)
		}
		stm.Put(cnEntityKey, string(newCnEntityVal))
		return nil
	}
	err := po.sw.RunStm(apply, ctx, "CreateSnap: "+req.DaName+" "+req.SnapName)

	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.CreateSnapReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.CreateSnapReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	}

	po.sm.SyncupCn(primarySockAddr, ctx)

	return &pbpo.CreateSnapReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}

func isSnapDeletable(targetIdx int, snapList []*pbds.Snap) string {
	return ""
}

func (po *portalServer) DeleteSnap(ctx context.Context, req *pbpo.DeleteSnapRequest) (*pbpo.DeleteSnapReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	} else if req.SnapName == "" {
		invalidParamMsg = "SnapName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.DeleteSnapReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId: lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg: invalidParamMsg,
			},
		}, nil
	}

	daEntityKey := po.kf.DaEntityKey(req.DaName)
	diskArray := &pbds.DiskArray{}

	var primarySockAddr string

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

		var targetSnap *pbds.Snap
		targetIdx := -1
		for i, snap := range diskArray.SnapList {
			if snap.SnapName == req.SnapName {
				targetSnap = snap
				targetIdx = i
				break
			}
		}
		if targetSnap == nil {
			return &portalError{
				lib.PortalUnknownResErrCode,
				req.SnapName,
			}
		}
		invalidParamMsg = isSnapDeletable(targetIdx, diskArray.SnapList)
		if invalidParamMsg != "" {
			return fmt.Errorf("request parameter err: %v", invalidParamMsg)
		}
		length := len(diskArray.SnapList)
		diskArray.SnapList[targetIdx] = diskArray.SnapList[length-1]
		diskArray.SnapList = diskArray.SnapList[:length-1]
		newDaEntityVal, err := proto.Marshal(diskArray)
		if err != nil {
			logger.Error("Marshal diskArray err: %v %v", diskArray, err)
			return fmt.Errorf("marshal diskArray err: %v", err)
		}
		stm.Put(daEntityKey, string(newDaEntityVal))

		for _, cntlr := range diskArray.CntlrList {
			if cntlr.IsPrimary {
				controllerNode := &pbds.ControllerNode{}
				cnEntityKey := po.kf.CnEntityKey(cntlr.CnSockAddr)
				cnEntityVal := []byte(stm.Get(cnEntityKey))
				if err := proto.Unmarshal(cnEntityVal, controllerNode); err != nil {
					logger.Error("Unmarshal controllerNode err: %s %v", cnEntityKey, err)
					return err
				}
				primarySockAddr = controllerNode.SockAddr
				var targetCntlrFe *pbds.CntlrFrontend
				for _, cntlrFe := range controllerNode.CntlrFeList {
					if cntlrFe.CntlrId == cntlr.CntlrId {
						targetCntlrFe = cntlrFe
						break
					}
				}
				if targetCntlrFe == nil {
					logger.Error("Can not find cntlr: %v %v", cntlr, controllerNode)
					return fmt.Errorf("Can not find cntlr: %s %s",
						cntlr.CntlrId, controllerNode.SockAddr)
				}
				targetIdx := -1
				for i, snapFe := range targetCntlrFe.SnapFeList {
					if snapFe.SnapId == targetSnap.SnapId {
						targetIdx = i
					}
				}
				if targetIdx == -1 {
					logger.Error("Can not find snapFe: %v %v", targetSnap, controllerNode)
					return fmt.Errorf("Can not find snapFe: %s %s",
						targetSnap.SnapName, controllerNode.SockAddr)
				}
				length := len(targetCntlrFe.SnapFeList)
				targetCntlrFe.SnapFeList[targetIdx] = targetCntlrFe.SnapFeList[length-1]
				targetCntlrFe.SnapFeList = targetCntlrFe.SnapFeList[:length-1]
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
			}
		}
		return nil
	}
	err := po.sw.RunStm(apply, ctx, "DeleteSnap: " + req.DaName + "" + req.SnapName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.DeleteSnapReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId: lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg: serr.msg,
				},
			}, nil
		} else {
			return &pbpo.DeleteSnapReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	}

	po.sm.SyncupCn(primarySockAddr, ctx)

	return &pbpo.DeleteSnapReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId: lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}

func (po *portalServer) modifySnapDescription(ctx context.Context, daName string,
	snapName string, description string) (*pbpo.ModifySnapReply, error) {
	daEntityKey := po.kf.DaEntityKey(daName)
	diskArray := &pbds.DiskArray{}

	apply := func(stm concurrency.STM) error {
		dnEntityVal := []byte(stm.Get(daEntityKey))
		if len(dnEntityVal) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				daEntityKey,
			}
		}
		if err := proto.Unmarshal(dnEntityVal, diskArray); err != nil {
			logger.Error("Unmarshal diskArray err: %s %v", daEntityKey, err)
			return err
		}
		var targetSnap *pbds.Snap
		for _, snap := range diskArray.SnapList {
			if snap.SnapName == snapName {
				targetSnap = snap
				break
			}
		}
		if targetSnap == nil {
			return &portalError{
				lib.PortalUnknownResErrCode,
				snapName,
			}
		}
		targetSnap.Description = description
		newDaEntityVal, err := proto.Marshal(diskArray)
		if err != nil {
			logger.Error("Marshal diskArray err: %v %v", diskArray, err)
			return err
		}
		stm.Put(daEntityKey, string(newDaEntityVal))
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "ModifySnap: "+daName+" "+snapName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.ModifySnapReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.ModifySnapReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	}

	return &pbpo.ModifySnapReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}

func (po *portalServer) ModifySnap(ctx context.Context, req *pbpo.ModifySnapRequest) (
	*pbpo.ModifySnapReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	} else if req.SnapName == "" {
		invalidParamMsg = "SnapName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.ModifySnapReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}
	switch x := req.Attr.(type) {
	case *pbpo.ModifySnapRequest_Description:
		return po.modifySnapDescription(ctx, req.DaName, req.SnapName, x.Description)
	default:
		logger.Error("Unknow attr: %v", x)
		return &pbpo.ModifySnapReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  "Unknow attr",
			},
		}, nil
	}
}

func (po *portalServer) ListSnap(ctx context.Context, req *pbpo.ListSnapRequest) (
	*pbpo.ListSnapReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.ListSnapReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	daEntityKey := po.kf.DaEntityKey(req.DaName)
	diskArray := &pbds.DiskArray{}
	snapSummaryList := make([]*pbpo.SnapSummary, 0)
	apply := func(stm concurrency.STM) error {
		dnEntityVal := []byte(stm.Get(daEntityKey))
		if len(dnEntityVal) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				daEntityKey,
			}
		}
		if err := proto.Unmarshal(dnEntityVal, diskArray); err != nil {
			logger.Error("Unmarshal diskArray err: %s %v", daEntityKey, err)
			return err
		}
		snapSummaryList = make([]*pbpo.SnapSummary, 0)
		for _, snap := range diskArray.SnapList {
			snapSummary := &pbpo.SnapSummary{
				SnapName: snap.SnapName,
				Description: snap.Description,
			}
			snapSummaryList = append(snapSummaryList, snapSummary)
		}
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "ListSnap: "+req.DaName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.ListSnapReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.ListSnapReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.ListSnapReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
			SnapSummaryList: snapSummaryList,
		}, nil
	}
}

func (po *portalServer) GetSnap(ctx context.Context, req *pbpo.GetSnapRequest) (
	*pbpo.GetSnapReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	} else if req.SnapName == "" {
		invalidParamMsg = "SnapName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.GetSnapReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	daEntityKey := po.kf.DaEntityKey(req.DaName)
	diskArray := &pbds.DiskArray{}
	var snap *pbpo.Snap

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
		var targetSnap *pbds.Snap
		for _, snap := range diskArray.SnapList {
			if snap.SnapName == req.SnapName {
				targetSnap = snap
				break
			}
		}
		if targetSnap == nil {
			return &portalError{
				lib.PortalUnknownResErrCode,
				req.SnapName,
			}
		}

		var targetCntlr *pbds.Controller
		for _,  cntlr := range diskArray.CntlrList {
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
			logger.Error("Unmarshal controllerNode err: %s %v", cnEntityKey, err)
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
		var targetSnapFe *pbds.SnapFrontend
		for _, snapFe := range targetCntlrFe.SnapFeList {
			if snapFe.SnapId == targetSnap.SnapId {
				targetSnapFe = snapFe
				break
			}
		}
		if targetSnapFe == nil {
			logger.Error("Can not find snapFe: %v %v",
				targetSnap, controllerNode)
			return fmt.Errorf("Can not find snapFe: %s %s",
				targetSnap.SnapName, controllerNode.SockAddr)
		}
		snap = &pbpo.Snap{
			SnapId: targetSnap.SnapId,
			SnapName: targetSnap.SnapName,
			Description: targetSnap.Description,
			OriName: targetSnap.SnapName,
			Idx: targetSnap.Idx,
			Size: targetSnap.Size,
			ErrInfo: &pbpo.ErrInfo{
				IsErr: targetSnapFe.SnapFeInfo.ErrInfo.IsErr,
				ErrMsg: targetSnapFe.SnapFeInfo.ErrInfo.ErrMsg,
				Timestamp: targetSnapFe.SnapFeInfo.ErrInfo.Timestamp,
			},
		}
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "GetSnap: "+req.DaName+" "+req.SnapName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.GetSnapReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.GetSnapReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.GetSnapReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
			Snap: snap,
		}, nil
	}
}
