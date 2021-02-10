package monitor

import (
	// "context"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"

	"github.com/virtual-disk-array/vda/pkg/logger"
)

type workerI interface {
	getName() string
	getBacklogPrefix() string
	processBacklog(key string)
}

type manager struct {
	worker      workerI
	coord       *coordinator
	concurrency int
	interval    int
	etcdCli     *clientv3.Client
	quit        chan bool
	wg          sync.WaitGroup
}

func (man *manager) process() {
	total, current := man.coord.getTotalAndCurrent()
	if current == -1 {
		man.coord.initLease()
		man.coord.updateTotalAndCurrent()
		total, current = man.coord.getTotalAndCurrent()
	}
	if current == -1 {
		logger.Error("Can not get current, skip")
		return
	}
	logger.Info("Process: total=%d current=%d", total, current)
}

func (man *manager) run() {
	ticker := time.NewTicker(time.Duration(man.interval) * time.Second)
	man.wg.Add(1)
	go func() {
		defer man.wg.Done()
		for {
			select {
			case <-ticker.C:
				man.process()
			case <-man.quit:
				return
			}
		}
	}()
}

func (man *manager) close() {
	logger.Info("Closing %s", man.worker.getName())
	close(man.quit)
	man.wg.Wait()
	logger.Info("Closed %s", man.worker.getName())
}

func newManager(coord *coordinator, worker workerI,
	concurrency int, interval int, etcdCli *clientv3.Client) *manager {
	return &manager{
		worker:      worker,
		coord:       coord,
		concurrency: concurrency,
		interval:    interval,
		etcdCli:     etcdCli,
		quit:        make(chan bool),
	}
}
