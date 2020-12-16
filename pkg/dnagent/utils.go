package dnagent

import (
	"context"

	"google.golang.org/grpc"

	"github.com/virtual-disk-array/vda/pkg/logger"
)

func interceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	logger.Info("DnAgent call: %v", info.FullMethod)
	logger.Info("DnAgent request: %v", req)

	reply, err := handler(ctx, req)
	if err != nil {
		logger.Error("DnAgent error: %v", err)
	} else {
		logger.Info("DnAgent reply: %v", reply)
	}
	return reply, err
}
