package csi

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/mock/gomock"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/mocks/mockclient"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

func TestCreateVolume(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	name := "foo"
	size := int64(65536)

	mockPortalClient := mockclient.NewMockPortalClient(mockCtrl)
	createDaReply := pbpo.CreateDaReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     "xxx",
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}
	cs := newControllerServer(mockPortalClient)
	req := &csi.CreateVolumeRequest{
		Name: name,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: size,
		},
	}
	ctx := context.TODO()
	mockPortalClient.
		EXPECT().
		CreateDa(ctx, gomock.Any()).
		Return(&createDaReply, nil)
	resp, err := cs.CreateVolume(ctx, req)
	if err != nil {
		t.Errorf("CreateVolum err %v", err)
		return
	}
	if resp.Volume.VolumeId != name {
		t.Errorf("Wrong VolumeId: %s", resp.Volume.VolumeId)
		return
	}
	if resp.Volume.CapacityBytes != size {
		t.Errorf("Wrong size: %d", resp.Volume.CapacityBytes)
		return
	}
}
