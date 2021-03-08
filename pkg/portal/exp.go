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

func (po *portalServer) CreateExp(ctx context.Context, req *pbpo.CreateExpRequest) (
	*pbpo.CreateExpReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	} else if req.ExpName == "" {
		invalidParamMsg = "ExpName is empty"
	} else if req.InitiatorNqn == "" {
		invalidParamMsg = "InitiatorNqn is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.CreateExpReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	expId := lib.NewHexStrUuid()
	snapName := req.SnapName
	if snapName == "" {
		snapName = lib.DefaultSanpName
	}

	exp := &pbds.Exporter{
		ExpId:        expId,
		ExpName:      req.ExpName,
		Description:  req.Description,
		InitiatorNqn: req.InitiatorNqn,
	}

	daEntityKey := po.kf.DaEntityKey(req.DaName)
	diskArray := &pbds.DiskArray{}

	var primarySockAddr string
	cnSockAddrList := make([]string, 0)

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
		expFe := &pbds.ExpFrontend{
			ExpId: expId,
			ExpFeConf: &pbds.ExpFeConf{
				InitiatorNqn: req.InitiatorNqn,
				SnapId:       targetSnap.SnapId,
				DaName:       diskArray.DaName,
				ExpName:      req.ExpName,
			},
			ExpFeInfo: &pbds.ExpFeInfo{
				ErrInfo: &pbds.ErrInfo{
					IsErr:     true,
					ErrMsg:    lib.ResUninitMsg,
					Timestamp: lib.ResTimestamp(),
				},
			},
		}

		for _, exp1 := range diskArray.ExpList {
			if exp1.ExpName == req.ExpName {
				return &portalError{
					lib.PortalDupResErrCode,
					req.ExpName,
				}
			}
		}
		diskArray.ExpList = append(diskArray.ExpList, exp)
		newDaEntityVal, err := proto.Marshal(diskArray)
		if err != nil {
			logger.Error("Marshal diskArray err: %v %v", diskArray, err)
			return fmt.Errorf("Marshal diskArray err: %v", err)
		}
		stm.Put(daEntityKey, string(newDaEntityVal))

		for _, cntlr := range diskArray.CntlrList {
			controllerNode := &pbds.ControllerNode{}
			cnEntityKey := po.kf.CnEntityKey(cntlr.CnSockAddr)
			cnEntityVal := []byte(stm.Get(cnEntityKey))
			if err := proto.Unmarshal(cnEntityVal, controllerNode); err != nil {
				logger.Error("Unmarshal controllerNode err: %s %v", cnEntityKey, err)
				return err
			}
			var targetCntlrFe *pbds.CntlrFrontend
			for _, cntlrFe := range controllerNode.CntlrFeList {
				if cntlrFe.CntlrId == cntlr.CntlrId {
					targetCntlrFe = cntlrFe
					break
				}
			}
			if targetCntlrFe == nil {
				logger.Error("Can not find cntlr: %v %v", cntlr, controllerNode)
				return fmt.Errorf("Can not find cntlr")
			}
			if cntlr.IsPrimary {
				primarySockAddr = controllerNode.SockAddr
			} else {
				cnSockAddrList = append(cnSockAddrList, controllerNode.SockAddr)
			}
			targetCntlrFe.ExpFeList = append(targetCntlrFe.ExpFeList, expFe)
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
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "CreateExp: "+req.DaName+" "+req.ExpName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.CreateExpReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.CreateExpReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	}

	po.sm.SyncupCn(primarySockAddr, ctx)
	for _, sockAddr := range cnSockAddrList {
		po.sm.SyncupCn(sockAddr, ctx)
	}

	return &pbpo.CreateExpReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}

func (po *portalServer) DeleteExp(ctx context.Context, req *pbpo.DeleteExpRequest) (
	*pbpo.DeleteExpReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	} else if req.ExpName == "" {
		invalidParamMsg = "ExpName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.DeleteExpReply{
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
	cnSockAddrList := make([]string, 0)

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
		var targetExp *pbds.Exporter
		var targetIdx int
		for i, exp := range diskArray.ExpList {
			if exp.ExpName == req.ExpName {
				targetExp = exp
				targetIdx = i
				break
			}
		}
		if targetExp == nil {
			return &portalError{
				lib.PortalUnknownResErrCode,
				req.ExpName,
			}
		}
		length := len(diskArray.ExpList)
		diskArray.ExpList[targetIdx] = diskArray.ExpList[length-1]
		diskArray.ExpList = diskArray.ExpList[:length-1]
		newDaEntityVal, err := proto.Marshal(diskArray)
		if err != nil {
			logger.Error("Marshal diskArray err: %v %v", diskArray, err)
			return err
		}
		stm.Put(daEntityKey, string(newDaEntityVal))

		for _, cntlr := range diskArray.CntlrList {
			controllerNode := &pbds.ControllerNode{}
			cnEntityKey := po.kf.CnEntityKey(cntlr.CnSockAddr)
			cnEntityVal := []byte(stm.Get(cnEntityKey))
			if err := proto.Unmarshal(cnEntityVal, controllerNode); err != nil {
				logger.Error("Unmarshal controllerNode err: %s %v", cnEntityKey, err)
				return err
			}
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
			if cntlr.IsPrimary {
				primarySockAddr = controllerNode.SockAddr
			} else {
				cnSockAddrList = append(cnSockAddrList, controllerNode.SockAddr)
			}
			targetIdx := -1
			for i, expFe := range targetCntlrFe.ExpFeList {
				if expFe.ExpId == targetExp.ExpId {
					targetIdx = i
					break
				}
			}
			if targetIdx == -1 {
				logger.Error("Can not find expFe: %v %v", targetExp, controllerNode)
				return fmt.Errorf("Can not find expFe: %s %s",
					targetExp.ExpName, controllerNode.SockAddr)
			}
			length := len(targetCntlrFe.ExpFeList)
			targetCntlrFe.ExpFeList[targetIdx] = targetCntlrFe.ExpFeList[length-1]
			targetCntlrFe.ExpFeList = targetCntlrFe.ExpFeList[:length-1]
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
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "DeleteExp: "+req.DaName+" "+req.ExpName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.DeleteExpReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.DeleteExpReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	}

	po.sm.SyncupCn(primarySockAddr, ctx)
	for _, sockAddr := range cnSockAddrList {
		po.sm.SyncupCn(sockAddr, ctx)
	}

	return &pbpo.DeleteExpReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}

func (po *portalServer) modifyExpDescription(ctx context.Context, daName string,
	expName string, description string) (*pbpo.ModifyExpReply, error) {
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
		var targetExp *pbds.Exporter
		for _, exp := range diskArray.ExpList {
			if exp.ExpName == expName {
				targetExp = exp
				break
			}
		}
		if targetExp == nil {
			return &portalError{
				lib.PortalUnknownResErrCode,
				expName,
			}
		}
		targetExp.Description = description
		newDaEntityVal, err := proto.Marshal(diskArray)
		if err != nil {
			logger.Error("Marshal diskArray err: %v %v", diskArray, err)
			return err
		}
		stm.Put(daEntityKey, string(newDaEntityVal))
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "DeleteExp: "+daName+" "+expName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.ModifyExpReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.ModifyExpReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	}

	return &pbpo.ModifyExpReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}

func (po *portalServer) ModifyExp(ctx context.Context, req *pbpo.ModifyExpRequest) (
	*pbpo.ModifyExpReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	} else if req.ExpName == "" {
		invalidParamMsg = "ExpName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.ModifyExpReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}
	switch x := req.Attr.(type) {
	case *pbpo.ModifyExpRequest_Description:
		return po.modifyExpDescription(ctx, req.DaName, req.ExpName, x.Description)
	default:
		logger.Error("Unknow attr: %v", x)
		return &pbpo.ModifyExpReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  "Unknow attr",
			},
		}, nil
	}
}

func (po *portalServer) ListExp(ctx context.Context, req *pbpo.ListExpRequest) (
	*pbpo.ListExpReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.ListExpReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	daEntityKey := po.kf.DaEntityKey(req.DaName)
	diskArray := &pbds.DiskArray{}
	expSummaryList := make([]*pbpo.ExpSummary, 0)
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
		for _, exp := range diskArray.ExpList {
			expSummary := &pbpo.ExpSummary{
				ExpName:     exp.ExpName,
				Description: exp.Description,
			}
			expSummaryList = append(expSummaryList, expSummary)
		}
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "ListExp: "+req.DaName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.ListExpReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.ListExpReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.ListExpReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
			ExpSummaryList: expSummaryList,
		}, nil
	}
}

func (po *portalServer) GetExp(ctx context.Context, req *pbpo.GetExpRequest) (
	*pbpo.GetExpReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	} else if req.ExpName == "" {
		invalidParamMsg = "ExpName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.GetExpReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	daEntityKey := po.kf.DaEntityKey(req.DaName)
	diskArray := &pbds.DiskArray{}
	var exporter *pbpo.Exporter

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
		var targetExp *pbds.Exporter
		for _, exp := range diskArray.ExpList {
			if exp.ExpName == req.ExpName {
				targetExp = exp
				break
			}
		}
		if targetExp == nil {
			return &portalError{
				lib.PortalUnknownResErrCode,
				req.ExpName,
			}
		}

		exp_info_list := make([]*pbpo.ExpInfo, 0)
		for _, cntlr := range diskArray.CntlrList {
			controllerNode := &pbds.ControllerNode{}
			cnEntityKey := po.kf.CnEntityKey(cntlr.CnSockAddr)
			cnEntityVal := []byte(stm.Get(cnEntityKey))
			if err := proto.Unmarshal(cnEntityVal, controllerNode); err != nil {
				logger.Error("Unmarshal controllerNode err: %s %v", cnEntityKey, err)
				return err
			}
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
			var targetExpFe *pbds.ExpFrontend
			for _, expFe := range targetCntlrFe.ExpFeList {
				if expFe.ExpId == targetExp.ExpId {
					targetExpFe = expFe
					break
				}
			}
			if targetExpFe == nil {
				logger.Error("Can not find expFe: %v %v", targetExp, controllerNode)
				return fmt.Errorf("Can not find expFe: %s %s",
					targetExp.ExpName, controllerNode.SockAddr)
			}
			exp_info := &pbpo.ExpInfo{
				CntlrIdx: cntlr.CntlrIdx,
				NvmfListener: &pbpo.NvmfListener{
					TrType:  cntlr.CnNvmfListener.TrType,
					AdrFam:  cntlr.CnNvmfListener.AdrFam,
					TrAddr:  cntlr.CnNvmfListener.TrAddr,
					TrSvcId: cntlr.CnNvmfListener.TrSvcId,
				},
				ErrInfo: &pbpo.ErrInfo{
					IsErr:     targetExpFe.ExpFeInfo.ErrInfo.IsErr,
					ErrMsg:    targetExpFe.ExpFeInfo.ErrInfo.ErrMsg,
					Timestamp: targetExpFe.ExpFeInfo.ErrInfo.Timestamp,
				},
			}
			exp_info_list = append(exp_info_list, exp_info)
		}
		exporter = &pbpo.Exporter{
			ExpId:        targetExp.ExpId,
			ExpName:      targetExp.ExpName,
			Description:  targetExp.Description,
			InitiatorNqn: targetExp.InitiatorNqn,
			SnapName:     targetExp.SnapName,
			TargetNqn:    po.nf.ExpNqnName(req.DaName, req.ExpName),
			ExpInfoList:  exp_info_list,
		}
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "GetExp: "+req.DaName+" "+req.ExpName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.GetExpReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.GetExpReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.GetExpReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
			Exporter: exporter,
		}, nil
	}
}
