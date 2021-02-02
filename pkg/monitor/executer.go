package monitor

import (
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/coreos/etcd/clientv3"
	"github.com/spf13/cobra"
	// "google.golang.org/grpc"

	"github.com/virtual-disk-array/vda/pkg/logger"
	// pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

type monitorArgsStruct struct {
	etcdEndpoints       string
	dnHeartbeatWorkers  int
	dnHeartbeatInterval int
	dnSyncupWorkers     int
	dnSyncupInterval    int
	cnHeartbeatWorkers  int
	cnHeartbeatInterval int
	cnSyncupWorkers     int
	cnSyncupInterval    int
}

var (
	monitorCmd = &cobra.Command{
		Use:   "vda_monitor",
		Short: "vda monitor",
		Long:  `vda monitor`,
		Run:   launchMonitor,
	}
	monitorArgs = monitorArgsStruct{}
)

func init() {
	monitorCmd.PersistentFlags().StringVarP(
		&monitorArgs.etcdEndpoints, "etcd-endpoints", "", "localhost:2379",
		"etcd endpoint list")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.dnHeartbeatWorkers, "dn-heartbeat-workers", "", 100,
		"dn heartbeat workers")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.dnHeartbeatInterval, "dn-heartbeat-interval", "", 100,
		"dn heartbeat interval in seconds")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.dnSyncupWorkers, "dn-syncup-workers", "", 100,
		"dn syncup workers")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.dnSyncupInterval, "dn-syncup-interval", "", 100,
		"dn syncup interval in seconds")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.cnHeartbeatWorkers, "cn-heartbeat-workers", "", 100,
		"cn heartbeat workers")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.cnHeartbeatInterval, "cn-heartbeat-interval", "", 100,
		"cn heartbeat interval in seconds")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.cnSyncupWorkers, "cn-syncup-workers", "", 100,
		"cn syncup workers")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.cnSyncupInterval, "cn-syncup-interval", "", 100,
		"cn syncup intervoal in seconds")
}

func launchDnHeartbeat(etcdCli *clientv3.Client, workers int, interval int,
	quit chan bool, wg *sync.WaitGroup) {
	logger.Info("DnHeartbeat start")
	defer wg.Done()
	for {
		select {
		case <-quit:
			logger.Info("DnHeartbeat stop")
			return
		}
	}
}

func launchDnSyncup(etcdCli *clientv3.Client, workers int, interval int,
	quit chan bool, wg *sync.WaitGroup) {
	logger.Info("DnSyncup start")
	defer wg.Done()
	for {
		select {
		case <-quit:
			logger.Info("DnSyncup stop")
			return
		}
	}
}

func launchMonitor(cmd *cobra.Command, args []string) {
	logger.Info("monitorArgs: %v", monitorArgs)
	etcdEndpoints := strings.Split(monitorArgs.etcdEndpoints, ",")
	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: etcdEndpoints})
	if err != nil {
		logger.Fatal("Create etcd client err: %v", err)
	}
	defer etcdCli.Close()

	quit := make(chan bool)
	var wg sync.WaitGroup

	if monitorArgs.dnHeartbeatWorkers > 0 && monitorArgs.dnHeartbeatInterval > 0 {
		wg.Add(1)
		go launchDnHeartbeat(etcdCli,
			monitorArgs.dnHeartbeatWorkers, monitorArgs.dnHeartbeatInterval,
			quit, &wg)
	} else {
		logger.Info("Skip DnHeartbeat")
	}

	if monitorArgs.dnSyncupWorkers > 0 && monitorArgs.dnSyncupInterval > 0 {
		wg.Add(1)
		go launchDnSyncup(etcdCli,
			monitorArgs.dnSyncupWorkers, monitorArgs.dnSyncupInterval,
			quit, &wg)
	} else {
		logger.Info("Skip DnSyncup")
	}

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT,
		syscall.SIGUSR1, syscall.SIGUSR2)
	go func() {
		select {
		case s := <-c:
			logger.Info("Receive signal: %v", s)
			close(quit)
		}
	}()

	wg.Wait()
	logger.Info("Monitor stop")
}

func Execute() {
	if err := monitorCmd.Execute(); err != nil {
		logger.Fatal("Cmd execute err: %v", err)
	}
}
