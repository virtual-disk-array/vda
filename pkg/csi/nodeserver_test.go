package csi

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/mock/gomock"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/mocks/mockclient"
	"github.com/virtual-disk-array/vda/pkg/mocks/mockcsi"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

func TestNodeStageVolume(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockPortalClient := mockclient.NewMockPortalClient(mockCtrl)
	createExpReply := pbpo.CreateExpReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     "xxx",
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}
	mockPortalClient.
		EXPECT().
		CreateExp(gomock.Any(), gomock.Any()).
		Return(&createExpReply, nil)
	getExpReply := pbpo.GetExpReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     "xxx",
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
		Exporter: &pbpo.Exporter{
			ExpId:        "aaa",
			ExpName:      "bbb",
			Description:  "ccc",
			InitiatorNqn: "eee",
			SnapName:     "fff",
			TargetNqn:    "ggg",
			SerialNumber: "hhh",
			ModelNumber:  "iii",
			ExpInfoList: []*pbpo.ExpInfo{
				{
					CntlrIdx: 0,
					NvmfListener: &pbpo.NvmfListener{
						TrType:  "tcp",
						AdrFam:  "ipv4",
						TrAddr:  "127.0.0.1",
						TrSvcId: "4430",
					},
					ErrInfo: &pbpo.ErrInfo{
						IsErr:     false,
						ErrMsg:    "",
						Timestamp: lib.ResTimestamp(),
					},
				},
			},
		},
	}
	mockPortalClient.
		EXPECT().
		GetExp(gomock.Any(), gomock.Any()).
		Return(&getExpReply, nil)
	mockNodeOperator := mockcsi.NewMockNodeOperatorInterface(mockCtrl)
	mockNodeOperator.
		EXPECT().
		MountAndFormat(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)
	mockNodeOperator.
		EXPECT().
		CheckDeviceReady(gomock.Any()).
		Return(false, nil)
	mockNodeOperator.
		EXPECT().
		ExecWithTimeout(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)
	mockNodeOperator.
		EXPECT().
		WaitForDeviceReady(gomock.Any(), gomock.Any()).
		Return(nil)

	nodeId := "foo"
	volumeId := "bar"
	stagingTargetPath := "/tmp/baz"

	ns := newNodeServer(mockPortalClient, nodeId, mockNodeOperator)
	if ns == nil {
		t.Errorf("ns is nil")
		return
	}

	req := &csi.NodeStageVolumeRequest{
		VolumeId:          volumeId,
		StagingTargetPath: stagingTargetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType:     "ext4",
					MountFlags: []string{"a", "b"},
				},
			},
		},
	}

	ctx := context.TODO()

	_, err := ns.NodeStageVolume(ctx, req)
	if err != nil {
		t.Errorf("NodeStageVolume err: %v", err)
	}
}
