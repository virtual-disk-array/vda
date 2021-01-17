package portal

import (
	"context"
	// "encoding/base64"
	"fmt"
	"math/rand"

	// "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/golang/protobuf/proto"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

func (po *portalServer) CreateCn(ctx context.Context, req *pbpo.CreateCnRequest) (
	*pbpo.CreateCnReply, error) {
	invalidParamMsg := ""
	if req.SockAddr == "" {
		invalidParamMsg = "SockAddr is empty"
	} else if req.NvmfListener.TrType == "" {
		invalidParamMsg = "TrType is empty"
	} else if req.NvmfListener.AdrFam == "" {
		invalidParamMsg = "AdrFam is empty"
	} else if req.NvmfListener.TrAddr == "" {
		invalidParamMsg = "TrAddr is empty"
	} else if req.NvmfListener.TrSvcId == "" {
		invalidParamMsg = "TrSvcId is empty"
	} else if req.HashCode > lib.MaxHashCode {
		invalidParamMsg = fmt.Sprintf("HashCode is larger than %d", lib.MaxHashCode)
	}
	if invalidParamMsg != "" {
		return &pbpo.CreateCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	cnId := lib.NewHexStrUuid()

	hashCode := req.HashCode
	if hashCode == 0 {
		hashCode = uint32(rand.Intn(lib.MaxHashCode)) + 1
	}

	cnConf := &pbds.CnConf{
		Description: req.Description,
		NvmfListener: &pbds.NvmfListener{
			TrType:  req.NvmfListener.TrType,
			AdrFam:  req.NvmfListener.AdrFam,
			TrAddr:  req.NvmfListener.TrAddr,
			TrSvcId: req.NvmfListener.TrSvcId,
		},
		Location:  req.Location,
		IsOffline: req.IsOffline,
		HashCode:  hashCode,
	}
	cnInfo := &pbds.CnInfo{
		ErrInfo: &pbds.ErrInfo{
			IsErr:     true,
			ErrMsg:    lib.ResUninitMsg,
			Timestamp: lib.ResTimestamp(),
		},
	}
	cnCapacity := &pbds.CnCapacity{
		CntlrCnt: uint32(0),
	}
	cntlrFeList := make([]*pbds.CntlrFrontend, 0)
	controllerNode := &pbds.ControllerNode{
		CnId:        cnId,
		SockAddr:    req.SockAddr,
		CnConf:      cnConf,
		CnInfo:      cnInfo,
		CnCapacity:  cnCapacity,
		CntlrFeList: cntlrFeList,
	}
	cnSummary := &pbds.CnSummary{
		Description: req.Description,
	}
	cnEntityKey := po.kf.CnEntityKey(req.SockAddr)
	cnListKey := po.kf.CnListKey(hashCode, req.SockAddr)
	cnErrKey := po.kf.CnErrKey(hashCode, req.SockAddr)
	cnCapKey := po.kf.CnCapKey(cnCapacity.CntlrCnt, req.SockAddr)
	cnSearchAttr := &pbds.CnSearchAttr{
		CnCapacity: cnCapacity,
		Location:   req.Location,
	}

	cnEntityVal, err := proto.Marshal(controllerNode)
	if err != nil {
		logger.Error("Marshal controllerNode err: %v %v", controllerNode, err)
		return &pbpo.CreateCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	cnEntityValStr := string(cnEntityVal)

	cnListVal, err := proto.Marshal(cnSummary)
	if err != nil {
		logger.Error("Marshal cnSummary err: %v %v", cnSummary, err)
		return &pbpo.CreateCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	cnListValStr := string(cnListVal)

	cnErrVal, err := proto.Marshal(cnSummary)
	if err != nil {
		logger.Error("Marshal cnSummary err: %v %v", cnSummary, err)
		return &pbpo.CreateCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	cnErrValStr := string(cnErrVal)

	cnCapVal, err := proto.Marshal(cnSearchAttr)
	if err != nil {
		logger.Error("Marshal cnSearchAttr err: %v %v", cnSearchAttr, err)
		return &pbpo.CreateCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	cnCapValStr := string(cnCapVal)

	apply := func(stm concurrency.STM) error {
		if val := []byte(stm.Get(cnEntityKey)); len(val) != 0 {
			return &portalError{
				code: lib.PortalDupResErrCode,
				msg:  cnEntityKey,
			}
		}
		stm.Put(cnEntityKey, cnEntityValStr)
		stm.Put(cnListKey, cnListValStr)
		stm.Put(cnErrKey, cnErrValStr)
		stm.Put(cnCapKey, cnCapValStr)
		return nil
	}

	err = po.sw.RunStm(apply, ctx, "CreateCn: "+req.SockAddr)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.CreateCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.CreateCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	}

	po.sm.SyncupCn(req.SockAddr, ctx)

	return &pbpo.CreateCnReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}

func (po *portalServer) DeleteCn(ctx context.Context, req *pbpo.DeleteCnRequest) (
	*pbpo.DeleteCnReply, error) {
	invalidParamMsg := ""
	if req.SockAddr == "" {
		invalidParamMsg = "SockAddr is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.DeleteCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	cnEntityKey := po.kf.CnEntityKey(req.SockAddr)
	controllerNode := &pbds.ControllerNode{}

	apply := func(stm concurrency.STM) error {
		cnEntityVal := []byte(stm.Get(cnEntityKey))
		if len(cnEntityVal) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				cnEntityKey,
			}
		}
		if err := proto.Unmarshal(cnEntityVal, controllerNode); err != nil {
			logger.Error("Unmarshal controllerNode err: %s %v",
				cnEntityKey, controllerNode)
			return err
		}
		if len(controllerNode.CntlrFeList) > 0 {
			return &portalError{
				lib.PortalResBusyErrCode,
				"controllerNode has controller(s)",
			}
		}
		stm.Del(cnEntityKey)
		cnListKey := po.kf.CnListKey(controllerNode.CnConf.HashCode, req.SockAddr)
		stm.Del(cnListKey)
		cnCapKey := po.kf.CnCapKey(controllerNode.CnCapacity.CntlrCnt, req.SockAddr)
		stm.Del(cnCapKey)
		if controllerNode.CnInfo.ErrInfo.IsErr {
			cnErrKey := po.kf.CnErrKey(controllerNode.CnConf.HashCode, req.SockAddr)
			stm.Del(cnErrKey)
		}
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "DeleteCn: "+req.SockAddr)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.DeleteCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.DeleteCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.DeleteCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
		}, nil
	}
}

func (po *portalServer) GetCn(ctx context.Context, req *pbpo.GetCnRequest) (
	*pbpo.GetCnReply, error) {
	cnEntityKey := po.kf.CnEntityKey(req.SockAddr)
	controllerNode := &pbds.ControllerNode{}

	apply := func(stm concurrency.STM) error {
		val := []byte(stm.Get(cnEntityKey))
		if len(val) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				cnEntityKey,
			}
		}
		err := proto.Unmarshal(val, controllerNode)
		return err
	}

	err := po.sw.RunStm(apply, ctx, "GetCn: "+req.SockAddr)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.GetCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.GetCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		poCntlrFeList := make([]*pbpo.CntlrFrontend, 0)
		for _, dsCntlrFe := range controllerNode.CntlrFeList {
			var thisCntlr *pbds.Controller
			for _, cntlr := range dsCntlrFe.CntlrFeConf.CntlrList {
				if cntlr.CntlrId == dsCntlrFe.CntlrId {
					thisCntlr = cntlr
					break
				}
			}
			if thisCntlr == nil {
				return &pbpo.GetCnReply{
					ReplyInfo: &pbpo.ReplyInfo{
						ReqId:     lib.GetReqId(ctx),
						ReplyCode: lib.PortalInternalErrCode,
						ReplyMsg:  fmt.Sprintf("Can not find cntlr: %s", dsCntlrFe.CntlrId),
					},
				}, nil
			}
			poGrpFeList := make([]*pbpo.GrpFrontend, 0)
			for _, dsGrpFe := range dsCntlrFe.GrpFeList {
				poVdFeList := make([]*pbpo.VdFrontend, 0)
				for _, dsVdFe := range dsGrpFe.VdFeList {
					poVdFe := &pbpo.VdFrontend{
						VdId:  dsVdFe.VdId,
						VdIdx: dsVdFe.VdFeConf.VdIdx,
						Size:  dsVdFe.VdFeConf.Size,
						ErrInfo: &pbpo.ErrInfo{
							IsErr:     dsVdFe.VdFeInfo.ErrInfo.IsErr,
							ErrMsg:    dsVdFe.VdFeInfo.ErrInfo.ErrMsg,
							Timestamp: dsVdFe.VdFeInfo.ErrInfo.Timestamp,
						},
					}
					poVdFeList = append(poVdFeList, poVdFe)
				}
				poGrpFe := &pbpo.GrpFrontend{
					GrpId:  dsGrpFe.GrpId,
					GrpIdx: dsGrpFe.GrpFeConf.GrpIdx,
					Size:   dsGrpFe.GrpFeConf.Size,
					ErrInfo: &pbpo.ErrInfo{
						IsErr:     dsGrpFe.GrpFeInfo.ErrInfo.IsErr,
						ErrMsg:    dsGrpFe.GrpFeInfo.ErrInfo.ErrMsg,
						Timestamp: dsGrpFe.GrpFeInfo.ErrInfo.Timestamp,
					},
					VdFeList: poVdFeList,
				}
				poGrpFeList = append(poGrpFeList, poGrpFe)
			}
			poSnapFeList := make([]*pbpo.SnapFrontend, 0)
			for _, dsSnapFe := range dsCntlrFe.SnapFeList {
				poSnapFe := &pbpo.SnapFrontend{
					SnapId: dsSnapFe.SnapId,
					ErrInfo: &pbpo.ErrInfo{
						IsErr:     dsSnapFe.SnapFeInfo.ErrInfo.IsErr,
						ErrMsg:    dsSnapFe.SnapFeInfo.ErrInfo.ErrMsg,
						Timestamp: dsSnapFe.SnapFeInfo.ErrInfo.Timestamp,
					},
				}
				poSnapFeList = append(poSnapFeList, poSnapFe)
			}
			poExpFeList := make([]*pbpo.ExpFrontend, 0)
			for _, dsExpFe := range dsCntlrFe.ExpFeList {
				poExpFe := &pbpo.ExpFrontend{
					ExpId: dsExpFe.ExpId,
					ErrInfo: &pbpo.ErrInfo{
						IsErr:     dsExpFe.ExpFeInfo.ErrInfo.IsErr,
						ErrMsg:    dsExpFe.ExpFeInfo.ErrInfo.ErrMsg,
						Timestamp: dsExpFe.ExpFeInfo.ErrInfo.Timestamp,
					},
				}
				poExpFeList = append(poExpFeList, poExpFe)
			}
			poCntlrFe := &pbpo.CntlrFrontend{
				CntlrId:   dsCntlrFe.CntlrId,
				DaName:    dsCntlrFe.CntlrFeConf.DaName,
				CntlrIdx:  thisCntlr.CntlrIdx,
				IsPrimary: thisCntlr.IsPrimary,
				ErrInfo: &pbpo.ErrInfo{
					IsErr:     dsCntlrFe.CntlrFeInfo.ErrInfo.IsErr,
					ErrMsg:    dsCntlrFe.CntlrFeInfo.ErrInfo.ErrMsg,
					Timestamp: dsCntlrFe.CntlrFeInfo.ErrInfo.Timestamp,
				},
				GrpFeList:  poGrpFeList,
				SnapFeList: poSnapFeList,
				ExpFeList:  poExpFeList,
			}
			poCntlrFeList = append(poCntlrFeList, poCntlrFe)
		}
		return &pbpo.GetCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
			ControllerNode: &pbpo.ControllerNode{
				CnId:        controllerNode.CnId,
				SockAddr:    controllerNode.SockAddr,
				Description: controllerNode.CnConf.Description,
				NvmfListener: &pbpo.NvmfListener{
					TrType:  controllerNode.CnConf.NvmfListener.TrType,
					AdrFam:  controllerNode.CnConf.NvmfListener.AdrFam,
					TrAddr:  controllerNode.CnConf.NvmfListener.TrAddr,
					TrSvcId: controllerNode.CnConf.NvmfListener.TrSvcId,
				},
				Location:  controllerNode.CnConf.Location,
				IsOffline: controllerNode.CnConf.IsOffline,
				HashCode:  controllerNode.CnConf.HashCode,
				ErrInfo: &pbpo.ErrInfo{
					IsErr:     controllerNode.CnInfo.ErrInfo.IsErr,
					ErrMsg:    controllerNode.CnInfo.ErrInfo.ErrMsg,
					Timestamp: controllerNode.CnInfo.ErrInfo.Timestamp,
				},
				CntlrFeList: poCntlrFeList,
			},
		}, nil
	}
}
