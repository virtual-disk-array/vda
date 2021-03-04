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
	currVersion := atomic.LoadUint64(&lastVersion)
	if req.Version > currVersion {
		return &pbcn.CnHeartbeatReply{
			ReplyInfo: &pbcn.ReplyInfo{
				ReplyCode: lib.CnOutOfSyncErrCode,
				ReplyMsg: fmt.Sprintf("received rev: %d, current rev: %d",
					req.Version, currVersion),
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
