package cnagent

import (
	"context"
	"sync/atomic"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbcn "github.com/virtual-disk-array/vda/pkg/proto/cnagentapi"
)

func (cnAgent *cnAgentServer) CnHeartbeat(ctx context.Context, req *pbcn.CnHeartbeatRequest) (
	*pbcn.CnHeartbeatReply, error) {
	currVersion := atomic.LoadUint64(&lastVersion)
	if req.Version > currVersion {
		logger.Warning("received rev: %d, current rev: %d",
			req.Version, currVersion)
	}
	return &pbcn.CnHeartbeatReply{
		ReplyInfo: &pbcn.ReplyInfo{
			ReplyCode: lib.CnSucceedCode,
			ReplyMsg:  lib.CnSucceedMsg,
		},
	}, nil
}
