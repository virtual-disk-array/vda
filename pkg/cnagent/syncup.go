package cnagent

import (
	"context"
	// "fmt"
	"sync"

	"github.com/virtual-disk-array/vda/pkg/lib"
	//  "github.com/virtual-disk-array/vda/pkg/logger"
	pbcn "github.com/virtual-disk-array/vda/pkg/proto/cnagentapi"
)

var (
	mu      sync.Mutex
	lastReq *pbcn.SyncupCnRequest
)

func newErrInfo(err error) *pbcn.ErrInfo {
	if err != nil {
		return &pbcn.ErrInfo{
			IsErr:     true,
			ErrMsg:    err.Error(),
			Timestamp: lib.ResTimestamp(),
		}
	} else {
		return &pbcn.ErrInfo{
			IsErr:     false,
			ErrMsg:    "",
			Timestamp: lib.ResTimestamp(),
		}
	}
}

type syncupHelper struct {
	lisConf      *lib.LisConf
	nf           *lib.NameFmt
	oc           *lib.OperationClient
	feNvmeMap    map[string]bool
	aggBdevMap   map[string]bool
	daLvsMap     map[string]bool
	expNqnMap    map[string]bool
	secNvmeMap   map[string]bool
	grpBdevMap   map[string]bool
	raid0BdevMap map[string]bool
}

func (sh *syncupHelper) syncupCn(cnReq *pbcn.CnReq) *pbcn.CnRsp {
	return nil
}

func newSyncupHelper(lisConf *lib.LisConf, nf *lib.NameFmt, sc *lib.SpdkClient) *syncupHelper {
	return &syncupHelper{
		lisConf:      lisConf,
		nf:           nf,
		oc:           lib.NewOperationClient(sc),
		feNvmeMap:    make(map[string]bool),
		aggBdevMap:   make(map[string]bool),
		daLvsMap:     make(map[string]bool),
		expNqnMap:    make(map[string]bool),
		secNvmeMap:   make(map[string]bool),
		grpBdevMap:   make(map[string]bool),
		raid0BdevMap: make(map[string]bool),
	}
}

func (cnAgent *cnAgentServer) SyncupCn(ctx context.Context, req *pbcn.SyncupCnRequest) (
	*pbcn.SyncupCnReply, error) {
	mu.Lock()
	defer mu.Unlock()
	if lastReq != nil && req.Revision < lastReq.Revision {
		return &pbcn.SyncupCnReply{
			ReplyInfo: &pbcn.ReplyInfo{
				ReplyCode: lib.CnOldRevErrCode,
				ReplyMsg:  lib.CnOldRevErrMsg,
			},
		}, nil
	}
	lastReq = req
	sh := newSyncupHelper(cnAgent.lisConf, cnAgent.nf, cnAgent.sc)

	cnRsp := sh.syncupCn(req.CnReq)
	return &pbcn.SyncupCnReply{
		ReplyInfo: &pbcn.ReplyInfo{
			ReplyCode: lib.CnSucceedCode,
			ReplyMsg:  lib.CnSucceedMsg,
		},
		CnRsp: cnRsp,
	}, nil
}
