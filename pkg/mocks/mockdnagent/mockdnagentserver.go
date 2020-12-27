package mockdnagent

import (
	"context"
	"net"
	"os"
	"testing"

	"google.golang.org/grpc"

	"github.com/virtual-disk-array/vda/pkg/lib"
	pbdn "github.com/virtual-disk-array/vda/pkg/proto/dnagentapi"
)

const (
	dnTotalSize = uint64(20 * 1024 * 1024 * 1024)
	dnFreeSize  = uint64(10 * 1024 * 1024 * 1024)
)

type dnAgentServer struct {
	pbdn.UnimplementedDnAgentServer
	syncupDnCnt int
}

func (dnAgent *dnAgentServer) SyncupDn(ctx context.Context, req *pbdn.SyncupDnRequest) (
	*pbdn.SyncupDnReply, error) {
	dnAgent.syncupDnCnt++
	dnReq := req.DnReq
	pdRspList := make([]*pbdn.PdRsp, 0)
	for _, pdReq := range dnReq.PdReqList {
		vdBeRspList := make([]*pbdn.VdBeRsp, 0)
		for _, vdBeReq := range pdReq.VdBeReqList {
			vdBeRsp := &pbdn.VdBeRsp{
				VdId: vdBeReq.VdId,
				VdBeInfo: &pbdn.VdBeInfo{
					ErrInfo: &pbdn.ErrInfo{
						IsErr:     false,
						ErrMsg:    "",
						Timestamp: lib.ResTimestamp(),
					},
				},
			}
			vdBeRspList = append(vdBeRspList, vdBeRsp)
		}
		pdRsp := &pbdn.PdRsp{
			PdId: pdReq.PdId,
			PdInfo: &pbdn.PdInfo{
				ErrInfo: &pbdn.ErrInfo{
					IsErr:     false,
					ErrMsg:    "",
					Timestamp: lib.ResTimestamp(),
				},
			},
			PdCapacity: &pbdn.PdCapacity{
				TotalSize: dnTotalSize,
				FreeSize:  dnFreeSize,
			},
			VdBeRspList: vdBeRspList,
		}
		pdRspList = append(pdRspList, pdRsp)
	}
	dnRsp := &pbdn.DnRsp{
		DnId: dnReq.DnId,
		DnInfo: &pbdn.DnInfo{
			ErrInfo: &pbdn.ErrInfo{
				IsErr:     false,
				ErrMsg:    "",
				Timestamp: lib.ResTimestamp(),
			},
		},
		PdRspList: pdRspList,
	}
	return &pbdn.SyncupDnReply{
		ReplyInfo: &pbdn.ReplyInfo{
			ReplyCode: lib.DnSucceedCode,
			ReplyMsg:  lib.DnSucceedMsg,
		},
		DnRsp: dnRsp,
	}, nil
}

type MockDnAgentServer struct {
	sockPath string
	server   *grpc.Server
	t        *testing.T
	dnAgent  *dnAgentServer
}

func (s *MockDnAgentServer) Stop() {
	s.server.GracefulStop()
	os.Remove(s.sockPath)
}

func (s *MockDnAgentServer) SyncupDnCnt() int {
	return s.dnAgent.syncupDnCnt
}

func (s *MockDnAgentServer) SockAddr() string {
	return "unix://" + s.sockPath
}

func NewMockDnAgentServer(sockPath string, t *testing.T) (*MockDnAgentServer, error) {
	os.Remove(sockPath)
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Errorf("dn agent listen err: %v", err)
		return nil, err
	}
	server := grpc.NewServer()
	dnAgent := &dnAgentServer{}
	pbdn.RegisterDnAgentServer(server, dnAgent)
	go func() {
		if err := server.Serve(lis); err != nil {
			t.Errorf("dn agent serve err: %v", err)
			lis.Close()
		}
	}()
	return &MockDnAgentServer{
		sockPath: sockPath,
		server:   server,
		t:        t,
	}, nil
}
