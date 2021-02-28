package monitor

import (
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/coreos/etcd/clientv3"
	"github.com/spf13/cobra"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
)

type monitorArgsStruct struct {
	etcdEndpoints          string
	dnHeartbeatConcurrency int
	dnHeartbeatInterval    int
	dnSyncupConcurrency    int
	dnSyncupInterval       int
	cnHeartbeatConcurrency int
	cnHeartbeatInterval    int
	cnSyncupConcurrency    int
	cnSyncupInterval       int
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
		&monitorArgs.dnHeartbeatConcurrency, "dn-heartbeat-concurrency", "", 100,
		"dn heartbeat workers")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.dnHeartbeatInterval, "dn-heartbeat-interval", "", 5,
		"dn heartbeat interval in seconds")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.dnSyncupConcurrency, "dn-syncup-concurrency", "", 100,
		"dn syncup workers")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.dnSyncupInterval, "dn-syncup-interval", "", 5,
		"dn syncup interval in seconds")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.cnHeartbeatConcurrency, "cn-heartbeat-concurrency", "", 100,
		"cn heartbeat workers")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.cnHeartbeatInterval, "cn-heartbeat-interval", "", 5,
		"cn heartbeat interval in seconds")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.cnSyncupConcurrency, "cn-syncup-concurrency", "", 100,
		"cn syncup workers")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.cnSyncupInterval, "cn-syncup-interval", "", 5,
		"cn syncup intervoal in seconds")
}

func launchMonitor(cmd *cobra.Command, args []string) {
	logger.Info("monitorArgs: %v", monitorArgs)
	etcdEndpoints := strings.Split(monitorArgs.etcdEndpoints, ",")
	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: etcdEndpoints})
	if err != nil {
		logger.Fatal("Create etcd client err: %v", err)
	}
	defer etcdCli.Close()

	kf := lib.NewKeyFmt(lib.DefaultEtcdPrefix)
	coord := newCoordinator(etcdCli, kf)
	logger.Info("coord: %v", coord)
	total, current := coord.getTotalAndCurrent()
	logger.Info("total=%d current=%d", total, current)

	gc := lib.NewGrpcCache(lib.GrpcCacheTTL, lib.GrpcCacheStep, lib.GrpcCacheInterval)

	managerList := make([]*manager, 0)

	if monitorArgs.dnHeartbeatConcurrency > 0 && monitorArgs.dnHeartbeatInterval > 0 {
		dhw := newDnHeartbeatWorker(etcdCli, kf, gc, 10, 10, 5)
		man := newManager(coord, dhw, etcdCli,
			monitorArgs.dnHeartbeatConcurrency, monitorArgs.dnHeartbeatInterval)
		man.run()
		managerList = append(managerList, man)
	} else {
		logger.Info("Skip DnHeartbeat")
	}

	if monitorArgs.dnSyncupConcurrency > 0 && monitorArgs.dnSyncupInterval > 0 {
		dsw := newDnSyncupWorker(etcdCli, kf, gc, 5)
		man := newManager(coord, dsw, etcdCli,
			monitorArgs.dnHeartbeatConcurrency, monitorArgs.dnHeartbeatInterval)
		man.run()
		managerList = append(managerList, man)
	} else {
		logger.Info("Skip DnSyncup")
	}

	if monitorArgs.cnHeartbeatConcurrency > 0 && monitorArgs.cnHeartbeatInterval > 0 {
		chw := newCnHeartbeatWorker(etcdCli, kf, gc, 10, 10, 5)
		man := newManager(coord, chw, etcdCli,
			monitorArgs.cnHeartbeatConcurrency, monitorArgs.cnHeartbeatInterval)
		man.run()
		managerList = append(managerList, man)
	} else {
		logger.Info("Skip CnHeartbeat")
	}

	if monitorArgs.cnSyncupConcurrency > 0 && monitorArgs.cnSyncupInterval > 0 {
		csw := newCnSyncupWorker(etcdCli, kf, gc, 5)
		man := newManager(coord, csw, etcdCli,
			monitorArgs.cnHeartbeatConcurrency, monitorArgs.cnHeartbeatInterval)
		man.run()
		managerList = append(managerList, man)
	} else {
		logger.Info("Skip CnSyncup")
	}

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT,
		syscall.SIGUSR1, syscall.SIGUSR2)
	select {
	case s := <-c:
		logger.Info("Receive signal: %v", s)
		for _, man := range managerList {
			man.close()
		}
		coord.close()
		gc.Close()
	}
	logger.Info("Monitor stop")
}

func Execute() {
	if err := monitorCmd.Execute(); err != nil {
		logger.Fatal("Cmd execute err: %v", err)
	}
}
