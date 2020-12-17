package cnagent

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
	logger.Info("CnAgent call: %v", info.FullMethod)
	logger.Info("CnAgent request: %v", req)

	reply, err := handler(ctx, req)
	if err != nil {
		logger.Error("CnAgent error: %v", err)
	} else {
		logger.Info("CnAgent reply: %v", reply)
	}
	return reply, err
}
