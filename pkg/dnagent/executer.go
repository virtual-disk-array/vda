package dnagent

import (
	"encoding/json"
	"net"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbdn "github.com/virtual-disk-array/vda/pkg/proto/dnagentapi"
)

type dnAgentArgsStruct struct {
	network     string
	address     string
	sockPath    string
	sockTimeout int
	trConf      string
	lisConf     string
}

var (
	dnAgentCmd = &cobra.Command{
		Use:   "vda_dn_agent",
		Short: "vda dn agent",
		Long:  `vda dn agent`,
		Run:   launchDnAgent,
	}
	dnAgentArgs = dnAgentArgsStruct{}
)

func init() {
	dnAgentCmd.PersistentFlags().StringVarP(
		&dnAgentArgs.network, "network", "", "tcp",
		"dn agent network")
	dnAgentCmd.PersistentFlags().StringVarP(
		&dnAgentArgs.address, "address", "", ":9720",
		"dn agent address")
	dnAgentCmd.PersistentFlags().StringVarP(
		&dnAgentArgs.sockPath, "sock-path", "", "/var/tmp/spdk.sock",
		"spdk application socket path")
	dnAgentCmd.PersistentFlags().IntVarP(
		&dnAgentArgs.sockTimeout, "sock-timeout", "", 10,
		"spdk application socket path")
	dnAgentCmd.PersistentFlags().StringVarP(
		&dnAgentArgs.trConf, "tr-conf", "", `{"trtype":"TCP"}`,
		"nvmf transport configuration")
	dnAgentCmd.PersistentFlags().StringVarP(
		&dnAgentArgs.lisConf, "lis-conf", "",
		`{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4420"}`,
		"nvmf listener configuration")
}

func launchDnAgent(cmd *cobra.Command, args []string) {
	logger.Info("dnAgentArgs: %v", dnAgentArgs)

	var trConf map[string]interface{}
	trConfB := []byte(dnAgentArgs.trConf)
	if err := json.Unmarshal(trConfB, &trConf); err != nil {
		logger.Fatal("Unmarshal trConf err: %v", err)
	}

	lisConf := &lib.LisConf{}
	lisConfB := []byte(dnAgentArgs.lisConf)
	if err := json.Unmarshal(lisConfB, lisConf); err != nil {
		logger.Fatal("Unmarshal lisConf err: %v", err)
	}

	lis, err := net.Listen(dnAgentArgs.network, dnAgentArgs.address)
	if err != nil {
		logger.Fatal("listen %v %v err: %v",
			dnAgentArgs.network, dnAgentArgs.address, err)
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(interceptor),
	}
	s := grpc.NewServer(opts...)

	dnAgent, err := newDnAgentServer(dnAgentArgs.sockPath, dnAgentArgs.sockTimeout,
		lisConf, trConf)
	if err != nil {
		logger.Fatal("Create dn agent server err: %v", err)
	}
	defer dnAgent.Stop()
	pbdn.RegisterDnAgentServer(s, dnAgent)
	if err := s.Serve(lis); err != nil {
		logger.Fatal("Launch dn agent server err: %v", err)
	}
}

func Execute() {
	if err := dnAgentCmd.Execute(); err != nil {
		logger.Fatal("Cmd execute failed: %v", err)
	}
}
