package portal

import (
	"context"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"google.golang.org/grpc"

	"github.com/virtual-disk-array/vda/pkg/lib"
	pbdn "github.com/virtual-disk-array/vda/pkg/proto/dnagentapi"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

const (
	etcdPort       = "30000"
	etcdDir        = "/tmp/vdatest.default.etcd"
	dnUnixSockPath = "/tmp/vda"
	dnSockAddr     = "unix://" + dnUnixSockPath
	dnTotalSize    = uint64(20 * 1024 * 1024 * 1024)
	dnFreeSize     = uint64(10 * 1024 * 1024 * 1024)
)

type dnAgentServer struct {
	pbdn.UnimplementedDnAgentServer
}

func (s *dnAgentServer) SyncupDn(ctx context.Context, req *pbdn.SyncupDnRequest) (
	*pbdn.SyncupDnReply, error) {
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

func launchMockDnAgent(t *testing.T) {
	os.Remove(dnUnixSockPath)
	lis, err := net.Listen("unix", dnUnixSockPath)
	if err != nil {
		t.Errorf("dn agent listen err: %v", err)
		return
	}
	s := grpc.NewServer()
	pbdn.RegisterDnAgentServer(s, &dnAgentServer{})
	if err := s.Serve(lis); err != nil {
		t.Errorf("dn agent serve err: %v", err)
	}
}

func prepareEtcd(t *testing.T) *embed.Etcd {
	err := os.RemoveAll(etcdDir)
	if err != nil {
		t.Errorf("remove dir err: %v", err)
		return nil
	}
	cfg := embed.NewConfig()
	cfg.Dir = etcdDir
	listenClientURL, err := url.Parse("http://127.0.0.1:" + etcdPort)
	if err != nil {
		t.Errorf("parse listenClientURL err: %v", err)
		return nil
	}
	cfg.LCUrls = []url.URL{*listenClientURL}
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Errorf("StartEtcd err: %v", err)
		return nil
	}
	return e
}

func TestCreateDn(t *testing.T) {
	go launchMockDnAgent(t)
	time.Sleep(time.Second)
	e := prepareEtcd(t)
	defer e.Close()

	endpoints := []string{"127.0.0.1:" + etcdPort}
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		t.Errorf("create etcd client err: %v", err)
		return
	}
	defer cli.Close()

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ps := newPortalServer(cli)
	req := &pbpo.CreateDnRequest{
		SockAddr:    dnSockAddr,
		Description: "create mock dn",
		NvmfListener: &pbpo.NvmfListener{
			TrType:  "tcp",
			AdrFam:  "ipv4",
			TrAddr:  "127.0.0.1",
			TrSvcId: "4420",
		},
	}
	reply, err := ps.CreateDn(ctx, req)
	if err != nil {
		t.Errorf("CreateDn err: %v", err)
		return
	}
	if reply.ReplyInfo.ReplyCode != lib.PortalSucceedCode {
		t.Errorf("Incorrect ReplyCode: %v", reply.ReplyInfo.ReplyCode)
	}

	// kv := clientv3.NewKV(cli)

	// opts := []clientv3.OpOption{
	// 	clientv3.WithPrefix(),
	// 	clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	// 	clientv3.WithLimit(3),
	// }
	// gr, err := kv.Get(ctx, "test", opts...)
	// if err != nil {
	// 	t.Errorf("get key err: %v", err)
	// 	return
	// }
	// cnt := len(gr.Kvs)
	// if cnt != 0 {
	// 	t.Errorf("get key result is not 0: %v %s", cnt, gr.Kvs[0].Value)
	// 	return
	// }

	// _, err = kv.Put(ctx, "test1", "foo")
	// if err != nil {
	// 	t.Errorf("put key err: %v", err)
	// 	return
	// }
}
