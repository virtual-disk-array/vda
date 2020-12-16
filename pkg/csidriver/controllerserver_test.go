package csidriver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/mock/gomock"

	"github.com/virtual-disk-array/vda/pkg/mocks/mockportalapi"
	pb "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

func TestCreateVolume(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	name := "foo"
	size := int64(65536)

	mockPortalClient := mockportalapi.NewMockPortalClient(mockCtrl)
	createDaRequest := pb.CreateDaRequest{
		DaName:       name,
		CntlrCnt:     1,
		DaSize:       uint64(size),
		PhysicalSize: uint64(size),
		DaConf:       `{"stripe_count":1, "stripe_size_kb":64}`,
	}
	createDaReply := pb.CreateDaReply{
		ReplyInfo: &pb.PortalReplyInfo{
			ReqId:     "xxx",
			ReplyCode: 0,
			ReplyMsg:  "",
		},
	}
	cs := newControllerServer(mockPortalClient)
	req := &csi.CreateVolumeRequest{
		Name:          name,
		CapacityRange: &csi.CapacityRange{RequiredBytes: size},
	}
	ctx := context.TODO()
	mockPortalClient.
		EXPECT().
		CreateDa(ctx, &createDaRequest).
		Return(&createDaReply, nil)
	resp, err := cs.CreateVolume(ctx, req)
	if err != nil {
		t.Error("CreateVolume err: ", err)
	}
	if resp.Volume.VolumeId != name {
		t.Error("Wrong name: ", resp.Volume.VolumeId)
	}
	if resp.Volume.CapacityBytes != size {
		t.Error("Wrong size: ", resp.Volume.CapacityBytes)
	}
}
