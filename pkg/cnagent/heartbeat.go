package cnagent

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/virtual-disk-array/vda/pkg/lib"
	pbcn "github.com/virtual-disk-array/vda/pkg/proto/cnagentapi"
)

func (cnAgent *cnAgentServer) CnHeartbeat(ctx context.Context, req *pbcn.CnHeartbeatRequest) (
	*pbcn.CnHeartbeatReply, error) {
	currRev := atomic.LoadInt64(&lastRev)
	if req.Revision > currRev {
		return &pbcn.CnHeartbeatReply{
			ReplyInfo: &pbcn.ReplyInfo{
				ReplyCode: lib.CnOutOfSyncErrCode,
				ReplyMsg: fmt.Sprintf("received rev: %d, current rev: %d",
					req.Revision, currRev),
			},
		}, nil
	} else {
		return &pbcn.CnHeartbeatReply{
			ReplyInfo: &pbcn.ReplyInfo{
				ReplyCode: lib.CnSucceedCode,
				ReplyMsg:  lib.CnSucceedMsg,
			},
		}, nil
	}
}
