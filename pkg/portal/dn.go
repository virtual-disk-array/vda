package portal

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/golang/protobuf/proto"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

func (po *portalServer) CreateDn(ctx context.Context, req *pbpo.CreateDnRequest) (
	*pbpo.CreateDnReply, error) {
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
		return &pbpo.CreateDnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	dnId := lib.NewHexStrUuid()

	hashCode := req.HashCode
	if hashCode == 0 {
		hashCode = uint32(rand.Intn(lib.MaxHashCode)) + 1
	}

	dnConf := &pbds.DnConf{
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
	dnInfo := &pbds.DnInfo{
		ErrInfo: &pbds.ErrInfo{
			IsErr:     true,
			ErrMsg:    lib.ResUninitMsg,
			Timestamp: lib.ResTimestamp(),
		},
	}
	diskNode := &pbds.DiskNode{
		DnId:     dnId,
		SockAddr: req.SockAddr,
		DnConf:   dnConf,
		DnInfo:   dnInfo,
	}

	dnSummary := &pbds.DnSummary{
		Description: req.Description,
	}

	dnEntityKey := po.kf.DnEntityKey(req.SockAddr)
	dnListKey := po.kf.DnListKey(hashCode, req.SockAddr)
	dnErrKey := po.kf.DnErrKey(hashCode, req.SockAddr)

	dnEntityVal, err := proto.Marshal(diskNode)
	if err != nil {
		logger.Error("Marshal diskNode failed: %v %v", diskNode, err)
		return &pbpo.CreateDnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	dnEntityValStr := string(dnEntityVal)

	dnListVal, err := proto.Marshal(dnSummary)
	if err != nil {
		logger.Error("Marshal dnSummary failed: %v %v", dnSummary, err)
		return &pbpo.CreateDnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	dnListValStr := string(dnListVal)

	dnErrVal, err := proto.Marshal(dnSummary)
	if err != nil {
		logger.Error("Marshal dnSummary failed: %v %v", dnSummary, err)
		return &pbpo.CreateDnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	dnErrValStr := string(dnErrVal)

	apply := func(stm concurrency.STM) error {
		if val := []byte(stm.Get(dnEntityKey)); len(val) != 0 {
			return &portalError{
				code: lib.PortalDupResErrCode,
				msg:  dnEntityKey,
			}
		}
		stm.Put(dnEntityKey, dnEntityValStr)
		stm.Put(dnListKey, dnListValStr)
		stm.Put(dnErrKey, dnErrValStr)
		return nil
	}

	err = po.sw.RunStm(apply, ctx, "CreateDn: "+req.SockAddr)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.CreateDnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.CreateDnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	}

	po.sm.SyncupDn(req.SockAddr, ctx)

	return &pbpo.CreateDnReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}

func (po *portalServer) GetDn(ctx context.Context, req *pbpo.GetDnRequest) (
	*pbpo.GetDnReply, error) {
	dnEntityKey := po.kf.DnEntityKey(req.SockAddr)
	diskNode := &pbds.DiskNode{}

	apply := func(stm concurrency.STM) error {
		val := []byte(stm.Get(dnEntityKey))
		if len(val) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				dnEntityKey,
			}
		}
		err := proto.Unmarshal(val, diskNode)
		return err
	}

	err := po.sw.RunStm(apply, ctx, "GetDn: "+req.SockAddr)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.GetDnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.GetDnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.GetDnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
			DiskNode: &pbpo.DiskNode{
				DnId:        diskNode.DnId,
				SockAddr:    diskNode.SockAddr,
				Description: diskNode.DnConf.Description,
				NvmfListener: &pbpo.NvmfListener{
					TrType:  diskNode.DnConf.NvmfListener.TrType,
					AdrFam:  diskNode.DnConf.NvmfListener.AdrFam,
					TrAddr:  diskNode.DnConf.NvmfListener.TrAddr,
					TrSvcId: diskNode.DnConf.NvmfListener.TrSvcId,
				},
				Location:  diskNode.DnConf.Location,
				IsOffline: diskNode.DnConf.IsOffline,
				HashCode:  diskNode.DnConf.HashCode,
				ErrInfo: &pbpo.ErrInfo{
					IsErr:     diskNode.DnInfo.ErrInfo.IsErr,
					ErrMsg:    diskNode.DnInfo.ErrInfo.ErrMsg,
					Timestamp: diskNode.DnInfo.ErrInfo.Timestamp,
				},
			},
		}, nil
	}
}
