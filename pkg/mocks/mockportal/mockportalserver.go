package mockportal

import (
	"context"
	"net"
	"os"
	"testing"

	"google.golang.org/grpc"

	"github.com/virtual-disk-array/vda/pkg/lib"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

type portalServer struct {
	pbpo.UnimplementedPortalServer
}

func (po *portalServer) CreateDn(ctx context.Context, req *pbpo.CreateDnRequest) (
	*pbpo.CreateDnReply, error) {
	return &pbpo.CreateDnReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}

type MockPortalServer struct {
	sockPath string
	server   *grpc.Server
	portal   *portalServer
	t        *testing.T
}

func (s *MockPortalServer) Stop() {
	s.server.GracefulStop()
	os.Remove(s.sockPath)
}

func (s *MockPortalServer) SockAddr() string {
	return "unix://" + s.sockPath
}

func NewMockPortalServer(sockPath string, t *testing.T) (*MockPortalServer, error) {
	os.Remove(sockPath)
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Errorf("portal agent listen err: %v", err)
		return nil, err
	}
	server := grpc.NewServer()
	portal := &portalServer{}
	pbpo.RegisterPortalServer(server, portal)
	go func() {
		if err := server.Serve(lis); err != nil {
			t.Errorf("portal serve err: %v", err)
			lis.Close()
		}
	}()
	return &MockPortalServer{
		sockPath: sockPath,
		server:   server,
		portal:   portal,
		t:        t,
	}, nil
}
