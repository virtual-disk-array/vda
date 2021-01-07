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

func (po *portalServer) listDnWithoutToken(ctx context.Context, limit int64) (
	*pbpo.ListDnReply, error) {
	opts := []clientv3.OpOption{
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(limit),
		clientv3.WithPrefix(),
	}
	kv := clientv3.NewKV(po.etcdCli)
	prefix := po.kf.DnListPrefix()
	gr, err := kv.Get(ctx, prefix, opts...)
	if err != nil {
		return &pbpo.ListDnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	dnSummaryList := make([]*pbpo.DnSummary, 0)
	for _, item := range gr.Kvs {
		_, sockAddr, err := po.kf.DecodeDnListKey(string(item.Key))
		if err != nil {
			return &pbpo.ListDnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
		dsDnSummary := &pbds.DnSummary{}
		if err := proto.Unmarshal(item.Value, dsDnSummary); err != nil {
			return &pbpo.ListDnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
		poDnSummary := &pbpo.DnSummary{
			SockAddr:    sockAddr,
			Description: dsDnSummary.Description,
		}
		dnSummaryList = append(dnSummaryList, poDnSummary)
	}
	token := ""
	if len(gr.Kvs) > 0 {
		lastKey := gr.Kvs[len(gr.Kvs)-1].Key
		token = base64.StdEncoding.EncodeToString(lastKey)
	}
	return &pbpo.ListDnReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
		Token:         token,
		DnSummaryList: dnSummaryList,
	}, nil

}

func (po *portalServer) listDnWithToken(ctx context.Context, limit int64,
	token string) (*pbpo.ListDnReply, error) {
	opts := []clientv3.OpOption{
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(limit),
		clientv3.WithFromKey(),
	}
	kv := clientv3.NewKV(po.etcdCli)
	lastKey, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return &pbpo.ListDnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	gr, err := kv.Get(ctx, string(lastKey), opts...)
	if err != nil {
		return &pbpo.ListDnReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
	}
	dnSummaryList := make([]*pbpo.DnSummary, 0)
	for _, item := range gr.Kvs[1:] {
		_, sockAddr, err := po.kf.DecodeDnListKey(string(item.Key))
		if err != nil {
			if serr, ok := err.(*lib.InvalidKeyError); ok {
				logger.Info("listDnWithToken InvalidKeyError: %v", serr)
				break
			} else {
				return &pbpo.ListDnReply{
					ReplyInfo: &pbpo.ReplyInfo{
						ReqId:     lib.GetReqId(ctx),
						ReplyCode: lib.PortalInternalErrCode,
						ReplyMsg:  err.Error(),
					},
				}, nil
			}
		}
		dsDnSummary := &pbds.DnSummary{}
		if err := proto.Unmarshal(item.Value, dsDnSummary); err != nil {
			return &pbpo.ListDnReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
		poDnSummary := &pbpo.DnSummary{
			SockAddr:    sockAddr,
			Description: dsDnSummary.Description,
		}
		dnSummaryList = append(dnSummaryList, poDnSummary)
	}
	nextToken := ""
	if len(gr.Kvs)-1 > 0 {
		lastKey := gr.Kvs[len(gr.Kvs)-1].Key
		nextToken = base64.StdEncoding.EncodeToString(lastKey)
	}
	return &pbpo.ListDnReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
		Token:         nextToken,
		DnSummaryList: dnSummaryList,
	}, nil
}

func (po *portalServer) ListDn(ctx context.Context, req *pbpo.ListDnRequest) (
	*pbpo.ListDnReply, error) {
	limit := lib.PortalDefaultListLimit
	if req.Limit > lib.PortalMaxListLimit {
		invalidParamMsg := fmt.Sprintf("Limit is larger than %d",
			lib.PortalMaxListLimit)
		return &pbpo.ListDnReply{
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
		return po.listDnWithoutToken(ctx, limit)
	} else {
		return po.listDnWithToken(ctx, limit, req.Token)
	}
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
