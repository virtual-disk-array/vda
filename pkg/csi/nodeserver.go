package csi

import (
	"context"
	"fmt"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"

	"github.com/virtual-disk-array/vda/pkg/lib"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

type NodeServer struct {
	csi.UnimplementedNodeServer
	vdaClient pbpo.PortalClient
	nodeId    string
	mux       sync.Mutex
	no        NodeOperatorInterface
}

func (ns *NodeServer) NodePublishVolume(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) (
	*csi.NodePublishVolumeResponse, error) {

	ns.mux.Lock()
	defer ns.mux.Unlock()

	stagingPath := req.GetStagingTargetPath()
	targetPath := req.GetTargetPath()
	fsType := req.GetVolumeCapability().GetMount().GetFsType()
	mntFlags := req.GetVolumeCapability().GetMount().GetMountFlags()
	mntFlags = append(mntFlags, "bind")
	klog.Infof("mount %s to %s, fstype: %s, flags: %v", stagingPath, targetPath, fsType, mntFlags)
	err := ns.no.Mount(stagingPath, targetPath, fsType, mntFlags)
	if err != nil {
		klog.Errorf("CreateMountPoint failed: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *NodeServer) NodeUnpublishVolume(
	ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest) (
	*csi.NodeUnpublishVolumeResponse, error) {

	ns.mux.Lock()
	defer ns.mux.Unlock()

	targetPath := req.GetTargetPath()
	err := ns.no.DeleteMountPoint(targetPath)
	if err != nil {
		klog.Errorf("DeleteMountPoint failed: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *NodeServer) NodeGetInfo(
	ctx context.Context,
	req *csi.NodeGetInfoRequest) (
	*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.nodeId,
	}, nil
}

func (ns *NodeServer) NodeGetCapabilities(
	ctx context.Context,
	req *csi.NodeGetCapabilitiesRequest) (
	*csi.NodeGetCapabilitiesResponse, error) {

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
				},
			},
		}},
	}, nil
}

func (ns *NodeServer) NodeStageVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest) (
	*csi.NodeStageVolumeResponse, error) {

	volumeId := req.GetVolumeId()
	klog.Infof("NodeStageVolume, volumeId: %v", volumeId)

	var err error

	initiatorNqn := getInitiatorNqn(ns.nodeId)

	createExpRequest := pbpo.CreateExpRequest{
		DaName:       volumeId,
		ExpName:      ns.nodeId,
		InitiatorNqn: initiatorNqn,
	}
	klog.Infof("CreateExpRequest: %v", createExpRequest)
	createExpReply, err := ns.vdaClient.CreateExp(ctx, &createExpRequest)
	if err != nil {
		klog.Errorf("CreateExp failed: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("CreateExp reply: %v", createExpReply)
	if createExpReply.ReplyInfo.ReplyCode != lib.PortalSucceedCode &&
		createExpReply.ReplyInfo.ReplyCode != lib.PortalDupResErrCode {
		klog.Errorf("CreateExp reply err: %v", createExpReply.ReplyInfo)
		return nil, status.Error(codes.Internal, createExpReply.ReplyInfo.ReplyMsg)
	}

	getExpRequest := pbpo.GetExpRequest{
		DaName:  volumeId,
		ExpName: ns.nodeId,
	}
	klog.Infof("GetExpRequest: %v", getExpRequest)

	ns.mux.Lock()
	defer ns.mux.Unlock()

	getExpReply, err := ns.vdaClient.GetExp(ctx, &getExpRequest)
	if err != nil {
		klog.Errorf("GetExp failed: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("GetExp reply: %v", getExpReply)
	if getExpReply.ReplyInfo.ReplyCode != lib.PortalSucceedCode {
		klog.Errorf("GetExp reply err: %v", getExpReply.ReplyInfo)
		return nil, status.Error(codes.Internal, getExpReply.ReplyInfo.ReplyMsg)
	}

	serialNumber := getExpReply.Exporter.SerialNumber
	targetNqn := getExpReply.Exporter.TargetNqn
	hostNqn := getExpReply.Exporter.InitiatorNqn
	targetAddr := getExpReply.Exporter.ExpInfoList[0].NvmfListener.TrAddr
	targetPort := getExpReply.Exporter.ExpInfoList[0].NvmfListener.TrSvcId

	devicePath := fmt.Sprintf("/dev/disk/by-id/nvme-VDA_CONTROLLER_%s", serialNumber)

	ready, err := ns.no.CheckDeviceReady(devicePath)
	if err != nil {
		klog.Errorf("Check device ready failed: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if !ready {
		cmdLine := []string{"nvme", "connect", "-t", "tcp",
			"-a", targetAddr, "-s", targetPort, "-n", targetNqn, "--hostnqn", hostNqn}
		err = ns.no.ExecWithTimeout(cmdLine, 40, ctx)
		if err != nil {
			klog.Errorf("Connect nvmf error: %v", err)
			return nil, status.Error(codes.Internal, err.Error())
		}

		err = ns.no.WaitForDeviceReady(devicePath, 10)
		if err != nil {
			klog.Errorf("Wait for nvmf error: %v", err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	stagingPath := req.GetStagingTargetPath() + "/" + volumeId
	fsType := req.GetVolumeCapability().GetMount().GetFsType()
	mntFlags := req.GetVolumeCapability().GetMount().GetMountFlags()
	klog.Infof("mount %s to %s, fstype: %s, flags: %v",
		devicePath, stagingPath, fsType, mntFlags)
	err = ns.no.MountAndFormat(devicePath, stagingPath, fsType, mntFlags)
	if err != nil {
		klog.Errorf("CreateMountPoint failed: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *NodeServer) NodeUnstageVolume(
	ctx context.Context,
	req *csi.NodeUnstageVolumeRequest) (
	*csi.NodeUnstageVolumeResponse, error) {
	volumeId := req.GetVolumeId()
	klog.Infof("NodeUnstageVolume, volumeId: %v", volumeId)

	ns.mux.Lock()
	defer ns.mux.Unlock()

	getExpRequest := pbpo.GetExpRequest{
		DaName:  volumeId,
		ExpName: ns.nodeId,
	}
	klog.Infof("GetExpRequest: %v", getExpRequest)
	getExpReply, err := ns.vdaClient.GetExp(ctx, &getExpRequest)
	if err != nil {
		klog.Errorf("GetExp failed: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("GetExp reply: %v", getExpReply)
	if getExpReply.ReplyInfo.ReplyCode != lib.PortalSucceedCode &&
		getExpReply.ReplyInfo.ReplyCode != lib.PortalUnknownResErrCode {
		klog.Errorf("GetExp reply err: %v", getExpReply.ReplyInfo)
		return nil, status.Error(codes.Internal, getExpReply.ReplyInfo.ReplyMsg)
	}
	if getExpReply.ReplyInfo.ReplyCode == lib.PortalUnknownResErrCode {
		klog.Infof("Exp has been delted: %v", getExpReply.ReplyInfo)
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	serialNumber := getExpReply.Exporter.SerialNumber
	targetNqn := getExpReply.Exporter.TargetNqn

	devicePath := fmt.Sprintf("/dev/disk/by-id/nvme-VDA_CONTROLLER_%s", serialNumber)
	stagingPath := req.GetStagingTargetPath() + "/" + volumeId

	err = ns.no.DeleteMountPoint(stagingPath)
	if err != nil {
		klog.Errorf("DeleteMountPoint failed: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	ready, err := ns.no.CheckDeviceReady(devicePath)
	if err != nil {
		klog.Errorf("CheckDeviceReady failed: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if ready {
		cmdLine := []string{"nvme", "disconnect", "-n", targetNqn}
		err := ns.no.ExecWithTimeout(cmdLine, 40, ctx)
		if err != nil {
			klog.Errorf("Disconnect nvmf error: %v", err)
			return nil, status.Error(codes.Internal, err.Error())
		}

		err = ns.no.WaitForDeviceGone(devicePath, 10)
		if err != nil {
			klog.Errorf("WaitForDeviceGone error: %v", err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	deleteExpRequest := pbpo.DeleteExpRequest{
		DaName:  volumeId,
		ExpName: ns.nodeId,
	}
	klog.Infof("DeleteExpRequest: %v", deleteExpRequest)
	deleteExpReply, err := ns.vdaClient.DeleteExp(ctx, &deleteExpRequest)
	if err != nil {
		klog.Errorf("DeleteExp failed: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.Infof("DeleteExp reply: %v", deleteExpReply)
	if deleteExpReply.ReplyInfo.ReplyCode != lib.PortalSucceedCode &&
		deleteExpReply.ReplyInfo.ReplyCode != lib.PortalUnknownResErrCode {
		klog.Errorf("DeleteExp reply err: %v", deleteExpReply.ReplyInfo)
		return nil, status.Error(codes.Internal, deleteExpReply.ReplyInfo.ReplyMsg)
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func newNodeServer(
	vdaClient pbpo.PortalClient, nodeId string, no NodeOperatorInterface) *NodeServer {
	return &NodeServer{
		vdaClient: vdaClient,
		nodeId:    nodeId,
		no:        no,
	}
}
