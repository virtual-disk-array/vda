package cnagent

import (
	"encoding/json"
	"net"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbcn "github.com/virtual-disk-array/vda/pkg/proto/cnagentapi"
)

type cnAgentArgsStruct struct {
	network     string
	address     string
	sockPath    string
	sockTimeout int
	trConf      string
	lisConf     string
}

var (
	cnAgentCmd = &cobra.Command{
		Use:   "vda_cn_agent",
		Short: "vda cn agent",
		Long:  `vda cn agent`,
		Run:   launchCnAgent,
	}
	cnAgentArgs = cnAgentArgsStruct{}
)

func init() {
	cnAgentCmd.PersistentFlags().StringVarP(
		&cnAgentArgs.network, "network", "", "tcp",
		"cn agent network")
	cnAgentCmd.PersistentFlags().StringVarP(
		&cnAgentArgs.address, "address", "", ":9720",
		"cn agent address")
	cnAgentCmd.PersistentFlags().StringVarP(
		&cnAgentArgs.sockPath, "sock-path", "", "/var/tmp/spdk.sock",
		"spdk application socket path")
	cnAgentCmd.PersistentFlags().IntVarP(
		&cnAgentArgs.sockTimeout, "sock-timeout", "", 10,
		"spdk application socket path")
	cnAgentCmd.PersistentFlags().StringVarP(
		&cnAgentArgs.trConf, "tr-conf", "", `{"trtype":"TCP"}`,
		"nvmf transport configuration")
	cnAgentCmd.PersistentFlags().StringVarP(
		&cnAgentArgs.lisConf, "lis-conf", "",
		`{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4430"}`,
		"nvmf listener configuration")
}

func launchCnAgent(cmd *cobra.Command, args []string) {
	logger.Info("cnAgentArgs: %v", cnAgentArgs)

	var trConf map[string]interface{}
	trConfB := []byte(cnAgentArgs.trConf)
	if err := json.Unmarshal(trConfB, &trConf); err != nil {
		logger.Fatal("Unmarshal trConf err: %v", err)
	}

	lisConf := &lib.LisConf{}
	lisConfB := []byte(cnAgentArgs.lisConf)
	if err := json.Unmarshal(lisConfB, lisConf); err != nil {
		logger.Fatal("Unmarshal lisConf err: %v", err)
	}

	lis, err := net.Listen(cnAgentArgs.network, cnAgentArgs.address)
	if err != nil {
		logger.Fatal("listen %v %v failed: %v",
			cnAgentArgs.network, cnAgentArgs.address, err)
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(interceptor),
	}
	s := grpc.NewServer(opts...)

	cnAgent, err := newCnAgentServer(cnAgentArgs.sockPath, cnAgentArgs.sockTimeout,
		lisConf, trConf)
	if err != nil {
		logger.Fatal("Create cn agent server err: %v", err)
	}
	defer cnAgent.Stop()
	pbcn.RegisterCnAgentServer(s, cnAgent)
	logger.Info("Launch cn agent server")
	if err := s.Serve(lis); err != nil {
		logger.Fatal("Launch cn agent server err: %v", err)
	}
}

func Execute() {
	if err := cnAgentCmd.Execute(); err != nil {
		logger.Fatal("Cmd execute err: %v", err)
	}
}
