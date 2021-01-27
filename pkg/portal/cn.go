package portal

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"

	"github.com/coreos/etcd/clientv3"
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
		SockAddr:    req.SockAddr,
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
		cnErrKey := po.kf.CnErrKey(controllerNode.CnConf.HashCode, req.SockAddr)
		if len(stm.Get(cnErrKey)) > 0 {
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

func (po *portalServer) modifyCnDescription(ctx context.Context, sockAddr string,
	description string) (*pbpo.ModifyCnReply, error) {
	cnEntityKey := po.kf.CnEntityKey(sockAddr)
	controllerNode := &pbds.ControllerNode{}
	cnSummary := &pbds.CnSummary{
		SockAddr:    sockAddr,
		Description: description,
	}
	cnListVal, err := proto.Marshal(cnSummary)
	if err != nil {
		logger.Error("Marshal cnSummary err: %v %v", cnSummary, err)
		return &pbpo.ModifyCnReply{
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
		return &pbpo.ModifyCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	cnErrValStr := string(cnErrVal)

	apply := func(stm concurrency.STM) error {
		cnEntityVal := []byte(stm.Get(cnEntityKey))
		if len(cnEntityVal) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				cnEntityKey,
			}
		}
		if err := proto.Unmarshal(cnEntityVal, controllerNode); err != nil {
			logger.Error("Unmarshal controllerNode err: %s", cnEntityKey)
			return err
		}
		controllerNode.CnConf.Description = description
		newCnEntityVal, err := proto.Marshal(controllerNode)
		if err != nil {
			logger.Error("Marshal controllerNode err: %s %v", cnEntityKey, controllerNode)
			return err
		}
		stm.Put(cnEntityKey, string(newCnEntityVal))
		cnListKey := po.kf.CnListKey(controllerNode.CnConf.HashCode, sockAddr)
		stm.Put(cnListKey, cnListValStr)
		cnErrKey := po.kf.CnErrKey(controllerNode.CnConf.HashCode, sockAddr)
		if len(stm.Get(cnErrKey)) > 0 {
			stm.Put(cnErrKey, cnErrValStr)
		}
		return nil
	}

	err = po.sw.RunStm(apply, ctx, "ModifyCnDescription: "+sockAddr)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.ModifyCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.ModifyCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.ModifyCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
		}, nil
	}
}

func (po *portalServer) modifyCnIsOffline(ctx context.Context, sockAddr string,
	isOffline bool) (*pbpo.ModifyCnReply, error) {
	cnEntityKey := po.kf.CnEntityKey(sockAddr)
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
			logger.Error("Unmarshal controllerNode err: %s", cnEntityKey)
			return err
		}
		controllerNode.CnConf.IsOffline = isOffline
		newCnEntityVal, err := proto.Marshal(controllerNode)
		if err != nil {
			logger.Error("Marshal controllerNode err: %s %v", cnEntityKey, controllerNode)
			return err
		}
		stm.Put(cnEntityKey, string(newCnEntityVal))
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "ModifyCnIsOffline: "+sockAddr)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.ModifyCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.ModifyCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.ModifyCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
		}, nil
	}
}

func (po *portalServer) modifyCnHashCode(ctx context.Context, sockAddr string,
	hashCode uint32) (*pbpo.ModifyCnReply, error) {
	cnEntityKey := po.kf.CnEntityKey(sockAddr)
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
			logger.Error("Unmarshal controllerNode err: %s", cnEntityKey)
			return err
		}
		oldHashCode := controllerNode.CnConf.HashCode
		controllerNode.CnConf.HashCode = hashCode
		newCnEntityVal, err := proto.Marshal(controllerNode)
		if err != nil {
			logger.Error("Marshal controllerNode err: %s %v", cnEntityKey, controllerNode)
			return err
		}
		stm.Put(cnEntityKey, string(newCnEntityVal))
		cnSummary := &pbds.CnSummary{
			SockAddr:    controllerNode.SockAddr,
			Description: controllerNode.CnConf.Description,
		}
		cnListVal, err := proto.Marshal(cnSummary)
		if err != nil {
			logger.Error("Marshal cnSummary err: %v %v", cnSummary, err)
			return err
		}
		oldCnListKey := po.kf.CnListKey(oldHashCode, sockAddr)
		stm.Del(oldCnListKey)
		newCnListKey := po.kf.CnListKey(hashCode, sockAddr)
		stm.Put(newCnListKey, string(cnListVal))
		if controllerNode.CnInfo.ErrInfo.IsErr {
			oldCnErrKey := po.kf.CnErrKey(oldHashCode, sockAddr)
			stm.Del(oldCnErrKey)
			cnErrVal, err := proto.Marshal(cnSummary)
			if err != nil {
				logger.Error("Marshal cnSummary err: %v %v", cnSummary, err)
				return err
			}
			newCnErrKey := po.kf.CnErrKey(hashCode, sockAddr)
			stm.Put(newCnErrKey, string(cnErrVal))
		}
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "ModifyCnHashCode: "+sockAddr)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.ModifyCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.ModifyCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.ModifyCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
		}, nil
	}
}

func (po *portalServer) ModifyCn(ctx context.Context, req *pbpo.ModifyCnRequest) (
	*pbpo.ModifyCnReply, error) {
	invalidParamMsg := ""
	if req.SockAddr == "" {
		invalidParamMsg = "SockAddr is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.ModifyCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	switch x := req.Attr.(type) {
	case *pbpo.ModifyCnRequest_Description:
		return po.modifyCnDescription(ctx, req.SockAddr, x.Description)
	case *pbpo.ModifyCnRequest_IsOffline:
		return po.modifyCnIsOffline(ctx, req.SockAddr, x.IsOffline)
	case *pbpo.ModifyCnRequest_HashCode:
		if x.HashCode == 0 {
			invalidParamMsg = "HashCode can not be 0"
		} else if x.HashCode > lib.MaxHashCode {
			invalidParamMsg = fmt.Sprintf("HashCode is larger than %d", lib.MaxHashCode)
		}
		if invalidParamMsg != "" {
			return &pbpo.ModifyCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInvalidParamCode,
					ReplyMsg:  invalidParamMsg,
				},
			}, nil
		}
		return po.modifyCnHashCode(ctx, req.SockAddr, x.HashCode)
	default:
		logger.Error("Unknow attr: %v", x)
		return &pbpo.ModifyCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  "Unknow attr",
			},
		}, nil
	}
}

func (po *portalServer) listCnWithoutToken(ctx context.Context, limit int64) (
	*pbpo.ListCnReply, error) {
	opts := []clientv3.OpOption{
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(limit + 1),
		clientv3.WithPrefix(),
	}
	kv := clientv3.NewKV(po.etcdCli)
	prefix := po.kf.CnListPrefix()
	gr, err := kv.Get(ctx, prefix, opts...)
	if err != nil {
		return &pbpo.ListCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	cnSummaryList := make([]*pbpo.CnSummary, 0)
	for _, item := range gr.Kvs {
		_, sockAddr, err := po.kf.DecodeCnListKey(string(item.Key))
		if err != nil {
			return &pbpo.ListCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
		dsCnSummary := &pbds.CnSummary{}
		if err := proto.Unmarshal(item.Value, dsCnSummary); err != nil {
			return &pbpo.ListCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
		poCnSummary := &pbpo.CnSummary{
			SockAddr:    sockAddr,
			Description: dsCnSummary.Description,
		}
		cnSummaryList = append(cnSummaryList, poCnSummary)
	}
	token := ""
	if len(gr.Kvs) > 0 {
		lastKey := gr.Kvs[len(gr.Kvs)-1].Key
		token = base64.StdEncoding.EncodeToString(lastKey)
	}
	return &pbpo.ListCnReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
		Token:         token,
		CnSummaryList: cnSummaryList,
	}, nil

}

func (po *portalServer) listCnWithToken(ctx context.Context, limit int64,
	token string) (*pbpo.ListCnReply, error) {
	opts := []clientv3.OpOption{
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(limit),
		clientv3.WithFromKey(),
	}
	kv := clientv3.NewKV(po.etcdCli)
	lastKey, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return &pbpo.ListCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	gr, err := kv.Get(ctx, string(lastKey), opts...)
	if err != nil {
		return &pbpo.ListCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	cnSummaryList := make([]*pbpo.CnSummary, 0)
	if len(gr.Kvs) <= 1 {
		return &pbpo.ListCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
			Token:         "",
			CnSummaryList: cnSummaryList,
		}, nil
	}
	for _, item := range gr.Kvs[1:] {
		_, sockAddr, err := po.kf.DecodeCnListKey(string(item.Key))
		if err != nil {
			if serr, ok := err.(*lib.InvalidKeyError); ok {
				logger.Info("listCnWithToken InvalidKeyError: %v", serr)
				break
			} else {
				return &pbpo.ListCnReply{
					ReplyInfo: &pbpo.ReplyInfo{
						ReqId:     lib.GetReqId(ctx),
						ReplyCode: lib.PortalInternalErrCode,
						ReplyMsg:  err.Error(),
					},
				}, nil
			}
		}
		dsCnSummary := &pbds.CnSummary{}
		if err := proto.Unmarshal(item.Value, dsCnSummary); err != nil {
			return &pbpo.ListCnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
		poCnSummary := &pbpo.CnSummary{
			SockAddr:    sockAddr,
			Description: dsCnSummary.Description,
		}
		cnSummaryList = append(cnSummaryList, poCnSummary)
	}
	nextToken := base64.StdEncoding.EncodeToString(gr.Kvs[len(gr.Kvs)-1].Key)
	return &pbpo.ListCnReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
		Token:         nextToken,
		CnSummaryList: cnSummaryList,
	}, nil
}

func (po *portalServer) ListCn(ctx context.Context, req *pbpo.ListCnRequest) (
	*pbpo.ListCnReply, error) {
	limit := lib.PortalDefaultListLimit
	if req.Limit > lib.PortalMaxListLimit {
		invalidParamMsg := fmt.Sprintf("Limit is larger than %d",
			lib.PortalMaxListLimit)
		return &pbpo.ListCnReply{
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
		return po.listCnWithoutToken(ctx, limit)
	} else {
		return po.listCnWithToken(ctx, limit, req.Token)
	}
}

func (po *portalServer) GetCn(ctx context.Context, req *pbpo.GetCnRequest) (
	*pbpo.GetCnReply, error) {
	invalidParamMsg := ""
	if req.SockAddr == "" {
		invalidParamMsg = "SockAddr is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.GetCnReply{
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

func (po *portalServer) SyncupCn(ctx context.Context, req *pbpo.SyncupCnRequest) (
	*pbpo.SyncupCnReply, error) {
	invalidParamMsg := ""
	if req.SockAddr == "" {
		invalidParamMsg = "SockAddr is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.SyncupCnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	po.sm.SyncupCn(req.SockAddr, ctx)

	return &pbpo.SyncupCnReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}
