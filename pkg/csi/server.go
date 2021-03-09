package csi

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
	"google.golang.org/grpc"
	"k8s.io/klog"

	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

const (
	driverName    = "csi.vda.io"
	driverVersion = "0.0.1"
)

func parseEndpoint(endpoint string) (proto, addr string, _ error) {
	if strings.HasPrefix(strings.ToLower(endpoint), "unix://") || strings.HasPrefix(strings.ToLower(endpoint), "tcp://") {
		s := strings.SplitN(endpoint, "://", 2)
		if s[1] != "" {
			return s[0], s[1], nil
		}
	}
	return "", "", fmt.Errorf("invalid endpoint: %v", endpoint)
}

func logGRPC(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	klog.Infof("GRPC call: %s", info.FullMethod)
	klog.Infof("GRPC request: %s", protosanitizer.StripSecrets(req))
	resp, err := handler(ctx, req)
	if err != nil {
		klog.Errorf("GRPC error: %v", err)
	} else {
		klog.Infof("GRPC response: %s", protosanitizer.StripSecrets(resp))
	}
	return resp, err
}

func StartGrpcServer(endpoint string, vdaEndpoint string,
	enableCs bool, enableNs bool, nodeId string) {
	klog.Infof("StartGrpcServer endpoint=%v vdaEndpoint=%v enableCs=%v enableNs=%v nodeId=%v",
		endpoint, vdaEndpoint, enableCs, enableNs, nodeId)
	var err error

	proto, addr, err := parseEndpoint(endpoint)
	if err != nil {
		klog.Fatal(err.Error())
		return
	}

	if proto == "unix" {
		addr = "/" + addr
		if err = os.Remove(addr); err != nil && !os.IsNotExist(err) {
			klog.Fatalf("Failed to remove %v, error: %v", addr, err)
			return
		}
	}

	lis, err := net.Listen(proto, addr)
	if err != nil {
		klog.Fatalf("Failed to listen: %v", err)
		return
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(logGRPC),
	}

	server := grpc.NewServer(opts...)

	ids := newIdentityServer(driverName, driverVersion)
	csi.RegisterIdentityServer(server, ids)

	if enableCs {
		conn, err := grpc.Dial(vdaEndpoint, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			klog.Errorf("Connect vda portal failed (CS): %v", err)
			return
		}
		defer conn.Close()
		client := pbpo.NewPortalClient(conn)
		cs := newControllerServer(client)
		csi.RegisterControllerServer(server, cs)
	}

	if enableNs {
		conn, err := grpc.Dial(vdaEndpoint, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			klog.Errorf("Connect vda portal failed (NS): %v", err)
			return
		}
		defer conn.Close()
		client := pbpo.NewPortalClient(conn)
		no := newNodeOperator()
		ns := newNodeServer(
			client, nodeId, no)
		csi.RegisterNodeServer(server, ns)
	}

	klog.Infof("listening for connections on address: %v", lis.Addr())

	err = server.Serve(lis)
	if err != nil {
		klog.Fatalf("Failed to start GRPC server: %v", err)
		return
	}
}
