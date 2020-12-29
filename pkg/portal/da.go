package portal

import (
	"context"
	"fmt"

	"github.com/coreos/etcd/clientv3/concurrency"
	// "github.com/golang/protobuf/proto"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	// pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

type retriableError struct {
	msg string
}

func (e *retriableError) Error() string {
	return e.msg
}

func (po *portalServer) applyAllocation(ctx context.Context, req *pbpo.CreateDaRequest,
	dnPdCandList []*lib.DnPdCand, cnCandList []*lib.CnCand) error {
	return nil
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

	vdSize := lib.DivCeil(req.PhysicalSize, uint64(req.StripCnt))
	qos := &lib.BdevQos{
		RwIosPerSec:    lib.DivCeil(req.RwIosPerSec, uint64(req.StripCnt)),
		RwMbytesPerSec: lib.DivCeil(req.RwMbytesPerSec, uint64(req.StripCnt)),
		RMbytesPerSec:  lib.DivCeil(req.RMbytesPerSec, uint64(req.StripCnt)),
		WMbytesPerSec:  lib.DivCeil(req.WMbytesPerSec, uint64(req.StripCnt)),
	}

	retryCnt := 0
	for {
		retryCnt++
		if retryCnt > lib.AllocMaxRetry {
			err = fmt.Errorf("Exceed max retry cnt")
			logger.Error("Exceed max retry cnt")
			return dnList, cnList, err
		}
		dnPdCandList, err := lib.AllocateDnPd(req.StripCnt, vdSize, qos)
		if err != nil {
			logger.Error("AllocateDnPd err: %v", err)
			return dnList, cnList, err
		}
		cnCandList, err := lib.AllocateCn(req.CntlrCnt)
		if err != nil {
			logger.Error("AllocateCn err: %v", err)
			return dnList, cnList, err
		}
		err = po.applyAllocation(ctx, req, dnPdCandList, cnCandList)
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
	} else if req.StripCnt == 0 {
		invalidParamMsg = "StripCnt is zero"
	} else if req.StripSizeKb == 0 {
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
		return &pbpo.CreateDaReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
		}, nil
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
