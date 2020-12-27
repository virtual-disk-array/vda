package portal

import (
	"context"
	"testing"
	"time"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/mocks/mockdnagent"
	"github.com/virtual-disk-array/vda/pkg/mocks/mocketcd"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

func TestCreateDn(t *testing.T) {
	sockPath := "/tmp/vdatestdn.sock"
	etcdPort := "30000"
	mockEtcd, err := mocketcd.NewMockEtcdServer(etcdPort, t)
	if err != nil {
		t.Errorf("Create mock etcd err: %v", err)
		return
	}
	defer mockEtcd.Stop()
	cli := mockEtcd.Client()

	mockDnAgent, err := mockdnagent.NewMockDnAgentServer(sockPath, t)
	if err != nil {
		t.Errorf("Create mock dn agent err: %v", err)
		return
	}
	defer mockDnAgent.Stop()
	sockAddr := mockDnAgent.SockAddr()

	time.Sleep(time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ps := newPortalServer(cli)
	req := &pbpo.CreateDnRequest{
		SockAddr:    sockAddr,
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
		t.Errorf("ReplyCode wrong: %v", reply.ReplyInfo.ReplyCode)
		return
	}

	syncupDnCnt := mockDnAgent.SyncupDnCnt()
	if syncupDnCnt != 1 {
		t.Errorf("syncupDnCnt wrong: %d", syncupDnCnt)
		return
	}
}
