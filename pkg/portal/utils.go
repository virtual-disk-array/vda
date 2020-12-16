package portal

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
)

func interceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	newCtx := lib.SetReqId(ctx)
	logger.Info("Portal call: %v", info.FullMethod)
	logger.Info("Portal request: %v", req)
	logger.Info("Portal reqId: %v", lib.GetReqId(newCtx))

	reply, err := handler(newCtx, req)
	if err != nil {
		logger.Error("Portal error: %v", err)
	} else {
		logger.Info("Portal reply: %v", reply)
	}
	return reply, err
}

type portalError struct {
	code uint32
	msg  string
}

func (pe *portalError) Error() string {
	return fmt.Sprintf("code: %d msg: %s", pe.code, pe.msg)
}

const (
	succeedCode       = 0
	succeedMsg        = "succeed"
	internalErrCode   = 1
	internalErrMsg    = "internal error"
	dupResErrCode     = 2
	dupResErrMsg      = "duplicate resource"
	unknownResErrCode = 3
	unknownResErrMsg  = "unknonwn resource"
)
