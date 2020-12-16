package main

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/virtual-disk-array/vda/pkg/csidriver"
	"k8s.io/klog"
)

type argsStruct struct {
	endpoint    string
	vdaEndpoint string
	enableCs    bool
	enableNs    bool
	nodeId      string
}

var (
	cmd = &cobra.Command{
		Use:   "vdacsi",
		Short: "vda csi controller and node driver",
	}
	args = argsStruct{}
)

func init() {
	cmd.PersistentFlags().StringVarP(
		&args.endpoint, "endpoint", "", "unix:///csi/csi-provisioner.sock", "endpoint")
	cmd.PersistentFlags().StringVarP(
		&args.vdaEndpoint, "vda-endpoint", "", "localhost:9520", "vda endpoint")
	cmd.PersistentFlags().BoolVarP(
		&args.enableCs, "enable-cs", "", false, "enable controller server")
	cmd.PersistentFlags().BoolVarP(
		&args.enableNs, "enable-ns", "", false, "enable node server")
	cmd.PersistentFlags().StringVarP(
		&args.nodeId, "node-id", "", "", "node id")
}

func main() {
	if err := cmd.Execute(); err != nil {
		klog.Errorf("Args err: %v", err)
		os.Exit(1)
	}
	klog.Infof("args: %v", args)
	csidriver.StartGrpcServer(
		args.endpoint, args.vdaEndpoint, args.enableCs, args.enableNs, args.nodeId)
}
