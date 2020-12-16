package csidriver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/mock/gomock"

	"github.com/virtual-disk-array/vda/pkg/mocks/mockcsidriver"
	"github.com/virtual-disk-array/vda/pkg/mocks/mockportalapi"
	pb "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

func TestNodeStageVolume(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockPortalClient := mockportalapi.NewMockPortalClient(mockCtrl)
	mockPortalClient.
		EXPECT().
		CreateExp(gomock.Any(), gomock.Any())

	getExpReply := pb.GetExpReply{
		ReplyInfo: &pb.PortalReplyInfo{
			ReqId:     "xxx",
			ReplyCode: 0,
			ReplyMsg:  "",
		},
		ExpMsg: &pb.ExpMsg{
			ExpId:        "aaa",
			ExpName:      "bbb",
			DaName:       "ccc",
			ExpNqn:       "ddd",
			InitiatorNqn: "eee",
			SnapName:     "fff",
			EsMsgList: []*pb.EsMsg{{
				EsId:           "ggg",
				CntlrIdx:       0,
				CnName:         "iii",
				CnListenerConf: `{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4430"}`,
				Error:          false,
				ErrorMsg:       "",
			}},
		},
	}
	mockPortalClient.
		EXPECT().
		GetExp(gomock.Any(), gomock.Any()).
		Return(&getExpReply, nil)
	mockNodeOperator := mockcsidriver.NewMockNodeOperatorInterface(mockCtrl)
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
		t.Error("ns is nil")
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
		t.Error("NodeStageVolume err: ", err)
	}
}
