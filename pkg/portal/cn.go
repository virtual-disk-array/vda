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
