package portal

import (
	"net"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/virtual-disk-array/vda/pkg/logger"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

type portalArgsStruct struct {
	etcdEndpoints string
	portalNetwork string
	portalAddress string
}

var (
	portalCmd = &cobra.Command{
		Use:   "vda_portal",
		Short: "vda portal",
		Long:  `vda portal`,
		Run:   launchPortal,
	}
	portalArgs = portalArgsStruct{}
)

func init() {
	portalCmd.PersistentFlags().StringVarP(
		&portalArgs.etcdEndpoints, "etcd-endpoints", "", "localhost:2379", "etcd endpoint list")
	portalCmd.PersistentFlags().StringVarP(
		&portalArgs.portalNetwork, "portal-network", "", "tcp", "portal network")
	portalCmd.PersistentFlags().StringVarP(
		&portalArgs.portalAddress, "portal-address", "", ":9520", "portal address")
}

func launchPortal(cmd *cobra.Command, args []string) {
	logger.Info("portalArgs: %v", portalArgs)
	etcdEndpoints := strings.Split(portalArgs.etcdEndpoints, ",")
	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: etcdEndpoints})
	if err != nil {
		logger.Fatal("Create etcd client failed: %v", err)
	}
	defer etcdCli.Close()

	lis, err := net.Listen(portalArgs.portalNetwork, portalArgs.portalAddress)
	if err != nil {
		logger.Fatal("listen %v %v failed: %v",
			portalArgs.portalNetwork, portalArgs.portalAddress, err)
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(interceptor),
	}
	s := grpc.NewServer(opts...)

	po := newPortalServer(etcdCli)
	pbpo.RegisterPortalServer(s, po)
	logger.Info("Launch portal server")
	if err := s.Serve(lis); err != nil {
		logger.Fatal("Launch portal server failed: %v", err)
	}
}

func Execute() {
	if err := portalCmd.Execute(); err != nil {
		logger.Fatal("Cmd execute failed: %v", err)
	}
}
