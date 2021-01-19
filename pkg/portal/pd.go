package portal

import (
	"context"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/golang/protobuf/proto"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

func (po *portalServer) CreatePd(ctx context.Context, req *pbpo.CreatePdRequest) (
	*pbpo.CreatePdReply, error) {
	invalidParamMsg := ""
	if req.SockAddr == "" {
		invalidParamMsg = "SockAddr is empty"
	} else if req.PdName == "" {
		invalidParamMsg = "PdName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.CreatePdReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	pdId := lib.NewHexStrUuid()
	pdConf := &pbds.PdConf{}
	pdConf.Description = req.Description
	pdConf.IsOffline = req.IsOffline

	switch x := req.BdevType.(type) {
	case *pbpo.CreatePdRequest_BdevMalloc:
		pdConf.BdevType = &pbds.PdConf_BdevMalloc{
			BdevMalloc: &pbds.BdevMalloc{
				Size: x.BdevMalloc.Size,
			},
		}
	case *pbpo.CreatePdRequest_BdevAio:
		pdConf.BdevType = &pbds.PdConf_BdevAio{
			BdevAio: &pbds.BdevAio{
				FileName: x.BdevAio.FileName,
			},
		}
	case *pbpo.CreatePdRequest_BdevNvme:
		pdConf.BdevType = &pbds.PdConf_BdevNvme{
			BdevNvme: &pbds.BdevNvme{
				TrAddr: x.BdevNvme.TrAddr,
			},
		}
	default:
		logger.Error("Unknow BdevType: %v", x)
		return &pbpo.CreatePdReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  "Unknow BdevType",
			},
		}, nil
	}

	pdInfo := &pbds.PdInfo{
		ErrInfo: &pbds.ErrInfo{
			IsErr:     true,
			ErrMsg:    lib.ResUninitMsg,
			Timestamp: lib.ResTimestamp(),
		},
	}

	physicalDisk := &pbds.PhysicalDisk{
		PdId:   pdId,
		PdName: req.PdName,
		PdConf: pdConf,
		PdInfo: pdInfo,
	}

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
		if err := proto.Unmarshal(val, diskNode); err != nil {
			logger.Error("Unmarshal diskNode err: %s %v", dnEntityKey, err)
			return err
		}

		if diskNode.SockAddr != req.SockAddr {
			logger.Error("SockAddr mismatch: %s %v", req.SockAddr, diskNode)
			return &portalError{
				lib.PortalInternalErrCode,
				"DiskNode SockAddr mismatch",
			}
		}
		for _, pd := range diskNode.PdList {
			if pd.PdName == physicalDisk.PdName {
				return &portalError{
					lib.PortalDupResErrCode,
					physicalDisk.PdName,
				}
			}
		}
		diskNode.PdList = append(diskNode.PdList, physicalDisk)
		dnEntityVal, err := proto.Marshal(diskNode)
		if err != nil {
			logger.Error("Marshal diskNode err: %v %v", diskNode, err)
			return err
		}
		dnEntityValStr := string(dnEntityVal)
		stm.Put(dnEntityKey, dnEntityValStr)

		dnErrKey := po.kf.DnErrKey(diskNode.DnConf.HashCode, diskNode.SockAddr)
		if len(stm.Get(dnErrKey)) == 0 {
			dnSummary := &pbds.DnSummary{
				SockAddr:    diskNode.SockAddr,
				Description: diskNode.DnConf.Description,
			}
			dnErrVal, err := proto.Marshal(dnSummary)
			if err != nil {
				logger.Error("marshal dnSummary err: %v %v", dnSummary, err)
				return err
			}
			dnErrValStr := string(dnErrVal)
			stm.Put(dnErrKey, dnErrValStr)
		}

		return nil
	}

	err := po.sw.RunStm(apply, ctx, "CreatePd: "+req.SockAddr+" "+req.PdName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.CreatePdReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.CreatePdReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	}

	po.sm.SyncupDn(req.SockAddr, ctx)

	return &pbpo.CreatePdReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}
