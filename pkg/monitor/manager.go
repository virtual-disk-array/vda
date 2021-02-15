package monitor

import (
	"context"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
)

type task struct {
	ctx context.Context
	key string
}

type workerI interface {
	getName() string
	getRange(begin, end int) (string, string)
	processBacklog(ctx context.Context, key string)
}

type manager struct {
	worker      workerI
	coord       *coordinator
	concurrency int
	interval    int
	etcdCli     *clientv3.Client
	quit        chan bool
	taskCh      chan *task
	wg          sync.WaitGroup
}

func (man *manager) process() {
	name := man.worker.getName()
	total, current := man.coord.getTotalAndCurrent()
	if current == -1 {
		man.coord.initLease()
		man.coord.updateTotalAndCurrent()
		total, current = man.coord.getTotalAndCurrent()
	}
	if current == -1 {
		logger.Error("%s can not get current, skip", name)
		return
	}
	step := (lib.MaxHashCode + total) / total
	begin := step * current
	end := begin + step
	if end > lib.MaxHashCode {
		end = lib.MaxHashCode
	}
	logger.Info("%s: total=%d current=%d begin=%d end=%d", name, total, current, begin, end)
	key, endKey := man.worker.getRange(begin, end)
	opts := []clientv3.OpOption{
		clientv3.WithRange(endKey),
	}
	ctx, _ := context.WithTimeout(context.Background(),
		time.Duration(man.interval)*time.Second)
	resp, err := man.etcdCli.Get(ctx, key, opts...)
	if err != nil {
		logger.Error("manager get err: %s %v", name, err)
		return
	}
	logger.Info("backlog count: %s %d", name, len(resp.Kvs))
	for _, ev := range resp.Kvs {
		t := &task{
			ctx: ctx,
			key: string(ev.Key),
		}
		man.taskCh <- t
	}
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

	for i := 0; i < man.concurrency; i++ {
		man.wg.Add(1)
		go func() {
			defer man.wg.Done()
			for {
				select {
				case t := <-man.taskCh:
					man.worker.processBacklog(t.ctx, t.key)
				case <-man.quit:
					return
				}
			}
		}()
	}
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
		taskCh:      make(chan *task),
	}
}
