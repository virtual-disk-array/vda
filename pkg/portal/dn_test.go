package portal

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/mocks/mockdnagent"
	"github.com/virtual-disk-array/vda/pkg/mocks/mocketcd"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

func TestCreateDn(t *testing.T) {
	sockPath := "/tmp/vdatestdn.sock"
	etcdPort := "30000"
	return
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

	po := newPortalServer(cli)
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
	reply, err := po.CreateDn(ctx, req)
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

	diskNode := &pbds.DiskNode{}
	dnEntityKey := po.kf.DnEntityKey(sockAddr)
	dnEntityVal, err := mockEtcd.Get(ctx, dnEntityKey)
	if err != nil {
		t.Errorf("Get %s err: %v", dnEntityKey, err)
		return
	}
	if dnEntityVal == nil {
		t.Errorf("Can not find val of %s", dnEntityKey)
		return
	}
	if err := proto.Unmarshal(dnEntityVal, diskNode); err != nil {
		t.Errorf("Unmarshal dnEntityVal err: %v", err)
		return
	}
	if diskNode.SockAddr != sockAddr {
		t.Errorf("diskNode SockAddr mismatch: %v", diskNode)
		return
	}
	if diskNode.DnInfo.ErrInfo.IsErr == true {
		t.Errorf("diskNode ErrInfo wrong: %v", diskNode)
	}
}

func TestGetDn(t *testing.T) {
	etcdPort := "30000"
	mockEtcd, err := mocketcd.NewMockEtcdServer(etcdPort, t)
	return
	if err != nil {
		t.Errorf("Create mock etcd err: %v", err)
		return
	}
	defer mockEtcd.Stop()
	cli := mockEtcd.Client()

	time.Sleep(time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	po := newPortalServer(cli)

	dnId := "25c55717-98cb-4959-8e60-6269d80a6b77"
	sockAddr := "localhost:9520"
	description := "disk node unittest"
	location := "127.0.0.1"
	diskNode := &pbds.DiskNode{
		DnId:     dnId,
		SockAddr: sockAddr,
		DnConf: &pbds.DnConf{
			Description: description,
			NvmfListener: &pbds.NvmfListener{
				TrType:  "tcp",
				AdrFam:  "ipv4",
				TrAddr:  "127.0.0.1",
				TrSvcId: "4420",
			},
			Location:  location,
			IsOffline: false,
			HashCode:  1,
		},
		DnInfo: &pbds.DnInfo{
			ErrInfo: &pbds.ErrInfo{
				IsErr:     false,
				ErrMsg:    "",
				Timestamp: lib.ResTimestamp(),
			},
		},
	}
	dnEntityKey := po.kf.DnEntityKey(sockAddr)
	dnEntityVal, err := proto.Marshal(diskNode)
	if err != nil {
		t.Errorf("Marshal diskNode err: %v %v", err, diskNode)
		return
	}
	if err := mockEtcd.Put(ctx, dnEntityKey, dnEntityVal); err != nil {
		t.Errorf("Put dnEntityKey err: %v %s", err, dnEntityKey)
		return
	}

	req := &pbpo.GetDnRequest{
		SockAddr: sockAddr,
	}
	reply, err := po.GetDn(ctx, req)
	if err != nil {
		t.Errorf("GetDn err: %v", err)
		return
	}
	if reply.ReplyInfo.ReplyCode != lib.PortalSucceedCode {
		t.Errorf("ReplyCode wrong: %v", reply.ReplyInfo.ReplyCode)
		return
	}
	if reply.DiskNode == nil {
		t.Errorf("Reply DiskNode is nil")
		return
	}
	if reply.DiskNode.DnId != dnId {
		t.Errorf("DnId mismatch: %v", reply.DiskNode)
		return
	}
	if reply.DiskNode.SockAddr != sockAddr {
		t.Errorf("SockAddr mismatch: %v", reply.DiskNode)
		return
	}
	errInfo := reply.DiskNode.ErrInfo
	if errInfo.IsErr != false {
		t.Errorf("ErrInfo mismatch: %v", reply.DiskNode)
		return
	}
}
