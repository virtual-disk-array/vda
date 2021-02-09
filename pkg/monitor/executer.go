package monitor

import (
	// "context"
	// "fmt"
	"os"
	"os/signal"
	"strings"
	// "sync"
	"syscall"
	// "time"

	"github.com/coreos/etcd/clientv3"
	"github.com/spf13/cobra"
	// "google.golang.org/grpc"
	// "github.com/google/uuid"

	// "github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	// pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
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
		&monitorArgs.dnHeartbeatInterval, "dn-heartbeat-interval", "", 100,
		"dn heartbeat interval in seconds")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.dnSyncupConcurrency, "dn-syncup-concurrency", "", 100,
		"dn syncup workers")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.dnSyncupInterval, "dn-syncup-interval", "", 100,
		"dn syncup interval in seconds")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.cnHeartbeatConcurrency, "cn-heartbeat-concurrency", "", 100,
		"cn heartbeat workers")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.cnHeartbeatInterval, "cn-heartbeat-interval", "", 100,
		"cn heartbeat interval in seconds")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.cnSyncupConcurrency, "cn-syncup-concurrency", "", 100,
		"cn syncup workers")
	monitorCmd.PersistentFlags().IntVarP(
		&monitorArgs.cnSyncupInterval, "cn-syncup-interval", "", 100,
		"cn syncup intervoal in seconds")
}

// func launchDnHeartbeat(etcdCli *clientv3.Client, workers int, interval int,
// 	quit chan bool, wg *sync.WaitGroup) {
// 	logger.Info("DnHeartbeat start")
// 	defer wg.Done()
// 	for {
// 		select {
// 		case <-quit:
// 			logger.Info("DnHeartbeat stop")
// 			return
// 		}
// 	}
// }

// func launchDnSyncup(etcdCli *clientv3.Client, workers int, interval int,
// 	quit chan bool, wg *sync.WaitGroup) {
// 	logger.Info("DnSyncup start")
// 	defer wg.Done()
// 	for {
// 		select {
// 		case <-quit:
// 			logger.Info("DnSyncup stop")
// 			return
// 		}
// 	}
// }

// type workerI interface {
// 	getName() string
// 	getRegPath() string
// 	getBacklogPrefix() string
// 	processBacklog(key string)
// }

// func launchWorker(etcdCli *clientv3.Client, worker workerI,
// 	concurrency int, interval int,
// 	quit chan bool, wg *sync.WaitGroup) {
// 	logger.Info("%s start", worker.getName())
// 	defer wg.Done()
// 	var mu sync.Mutex
// 	name := worker.getName()
// 	regPath := worker.getRegPath()
// 	workerId := uuid.New().String()
// 	regKey := fmt.Sprintf("%s/%s", regPath, workerId)
// 	logger.Info("worker: %s %s", name, regKey)

// 	kv := clientv3.NewKV(etcdCli)

// 	totalCnt := -1
// 	currIdx := -1

// 	updateMonitorInfo := func() {
// 		mu.Lock()
// 		defer mu.Unlock()
// 		totalCnt = -1
// 		currIdx = -1
// 		logger.Info("updateMonitorInfo: %s", name)
// 		opts := []clientv3.OpOption{
// 			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
// 			clientv3.WithLimit(lib.MaxHashCode + 1),
// 			clientv3.WithPrefix(),
// 		}
// 		ctx := context.Background()
// 		gr, err := kv.Get(ctx, regPath, opts...)
// 		if err != nil {
// 			logger.Error("Get regPath err: %v", err)
// 			return
// 		}

// 		total := len(gr.Kvs)
// 		curr := -1
// 		for i, item := range gr.Kvs {
// 			if string(item.Key) == regKey {
// 				curr = i
// 				break
// 			}
// 		}
// 		if curr == -1 {
// 			logger.Error("Can not find curr regKey")
// 			return
// 		}

// 		totalCnt = total
// 		currIdx = curr
// 	}

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		ctx := context.Background()
// 		watchChan := etcdCli.Watch(ctx, worker.getRegPath(), clientv3.WithPrefix())
// 		for {
// 			select {
// 			case <-watchChan:
// 				updateMonitorInfo()
// 			case <-quit:
// 				logger.Info("%s watch exit", name)
// 				return
// 			}
// 		}
// 	}()

// 	invokeWorker := func() {
// 		mu.Lock()
// 		total := totalCnt
// 		curr := currIdx
// 		mu.Unlock()
// 		logger.Info("total: %d curr: %d", total, curr)
// 	}

// 	ticker := time.NewTicker(time.Duration(interval) * time.Second)
// 	for {
// 		select {
// 		case <-ticker.C:
// 			invokeWorker()
// 		case <-quit:
// 			logger.Info("%s exit", name)
// 			return
// 		}
// 	}
// }

func launchMonitor(cmd *cobra.Command, args []string) {
	logger.Info("monitorArgs: %v", monitorArgs)
	etcdEndpoints := strings.Split(monitorArgs.etcdEndpoints, ",")
	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: etcdEndpoints})
	if err != nil {
		logger.Fatal("Create etcd client err: %v", err)
	}
	defer etcdCli.Close()

	coord := newCoordinator(etcdCli, "/vda/monitor")
	logger.Info("coord: %v", coord)

	// quit := make(chan bool)
	// var wg sync.WaitGroup

	// if monitorArgs.dnHeartbeatConcurrency > 0 && monitorArgs.dnHeartbeatInterval > 0 {
	// 	wg.Add(1)
	// 	dhw := newDnHeartbeatWorker()
	// 	go launchWorker(etcdCli, dhw,
	// 		monitorArgs.dnHeartbeatConcurrency, monitorArgs.dnHeartbeatInterval,
	// 		quit, &wg)
	// } else {
	// 	logger.Info("Skip DnHeartbeat")
	// }

	// if monitorArgs.dnSyncupConcurrency > 0 && monitorArgs.dnSyncupInterval > 0 {
	// 	wg.Add(1)
	// 	go launchDnSyncup(etcdCli,
	// 		monitorArgs.dnSyncupConcurrency, monitorArgs.dnSyncupInterval,
	// 		quit, &wg)
	// } else {
	// 	logger.Info("Skip DnSyncup")
	// }

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT,
		syscall.SIGUSR1, syscall.SIGUSR2)
	// go func() {
	// 	select {
	// 	case s := <-c:
	// 		logger.Info("Receive signal: %v", s)
	// 		close(quit)
	// 	}
	// }()

	select {
	case s := <-c:
		logger.Info("Receive signal: %v", s)
		coord.close()
	}
	// wg.Wait()
	logger.Info("Monitor stop")
}

func Execute() {
	if err := monitorCmd.Execute(); err != nil {
		logger.Fatal("Cmd execute err: %v", err)
	}
}
