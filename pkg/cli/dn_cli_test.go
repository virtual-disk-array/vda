package cli

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/mocks/mockportal"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

func TestCreateDnCli(t *testing.T) {
	sockPath := "/tmp/vdatestportal.sock"
	mockPortal, err := mockportal.NewMockPortalServer(sockPath, t)
	if err != nil {
		t.Errorf("Create mock portal err: %v", err)
		return
	}
	defer mockPortal.Stop()
	sockAddr := mockPortal.SockAddr()

	time.Sleep(time.Millisecond)

	rootArgs := &rootArgsStruct{
		portalAddr:    sockAddr,
		portalTimeout: 10,
	}
	cli := newClient(rootArgs)

	dnCreateArgs := &dnCreateArgsStruct{
		sockAddr:    "localhost:9720",
		description: "foo",
		trType:      "tcp",
		adrFam:      "ipv4",
		trAddr:      "127.0.0.1",
		trSvcId:     "4420",
		location:    "localhost:9720",
		isOffline:   false,
		hashCode:    0,
	}
	output := cli.createDn(dnCreateArgs)
	reply := &pbpo.CreateDnReply{}
	if err := json.Unmarshal([]byte(output), reply); err != nil {
		t.Errorf("Unmarshal reply err: %s %v", output, err)
		return
	}
	if reply.ReplyInfo.ReplyCode != lib.PortalSucceedCode {
		t.Errorf("Incorrect reply: %v", reply)
	}
}
