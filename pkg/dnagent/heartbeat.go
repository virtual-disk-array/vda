package dnagent

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/virtual-disk-array/vda/pkg/lib"
	pbdn "github.com/virtual-disk-array/vda/pkg/proto/dnagentapi"
)

func (dnAgent *dnAgentServer) DnHeartbeat(ctx context.Context, req *pbdn.DnHeartbeatRequest) (
	*pbdn.DnHeartbeatReply, error) {
	currRev := atomic.LoadInt64(&lastRev)
	if req.Revision > currRev {
		return &pbdn.DnHeartbeatReply{
			ReplyInfo: &pbdn.ReplyInfo{
				ReplyCode: lib.DnOutOfSyncErrCode,
				ReplyMsg: fmt.Sprintf("received rev: %d, current rev: %d",
					req.Revision, currRev),
			},
		}, nil
	} else {
		return &pbdn.DnHeartbeatReply{
			ReplyInfo: &pbdn.ReplyInfo{
				ReplyCode: lib.DnSucceedCode,
				ReplyMsg:  lib.DnSucceedMsg,
			},
		}, nil
	}
}
