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
	currVersion := atomic.LoadUint64(&lastVersion)
	if req.Version > currVersion {
		return &pbdn.DnHeartbeatReply{
			ReplyInfo: &pbdn.ReplyInfo{
				ReplyCode: lib.DnOutOfSyncErrCode,
				ReplyMsg: fmt.Sprintf("received rev: %d, current rev: %d",
					req.Version, currVersion),
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
