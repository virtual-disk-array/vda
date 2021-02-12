package monitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/google/uuid"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
)

const (
	timeoutSeconds = 10 * time.Second
)

type coordinator struct {
	etcdCli *clientv3.Client
	lease   *clientv3.Lease
	prefix  string
	id      string
	mu      sync.Mutex
	leaseId clientv3.LeaseID
	total   int
	current int
	quit    chan bool
	wg      sync.WaitGroup
}

func (coord *coordinator) getId() string {
	return coord.id
}

func (coord *coordinator) getTotalAndCurrent() (int, int) {
	coord.mu.Lock()
	total := coord.total
	current := coord.current
	coord.mu.Unlock()
	return total, current
}

func (coord *coordinator) close() {
	logger.Info("Closing coordinator")
	coord.mu.Lock()
	defer coord.mu.Unlock()
	close(coord.quit)
	coord.wg.Wait()
	if coord.leaseId != clientv3.NoLease {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutSeconds)
		_, err := coord.etcdCli.Revoke(ctx, coord.leaseId)
		cancel()
		if err != nil {
			logger.Warning("Revoke err: %v", err)
		}
	}
	logger.Info("Closed coordinator")
}

func (coord *coordinator) updateTotalAndCurrent() {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	coord.total = 0
	coord.current = -1
	ctx, cancel := context.WithTimeout(context.Background(), timeoutSeconds)
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(lib.MaxHashCode + 1),
	}
	resp, err := coord.etcdCli.Get(ctx, coord.prefix, opts...)
	cancel()
	if err != nil {
		logger.Error("updateTotalAndCurrent err: %v", err)
		return
	}
	total := len(resp.Kvs)
	current := -1
	key := fmt.Sprintf("%s/%s", coord.prefix, coord.id)
	for i, ev := range resp.Kvs {
		if string(ev.Key) == key {
			current = i
		}
	}
	coord.total = total
	coord.current = current
	logger.Info("updateTotalAndCurrent: total=%d current=%d", total, current)
}

func (coord *coordinator) initLease() error {
	coord.mu.Lock()
	defer coord.mu.Unlock()

	if coord.leaseId != clientv3.NoLease {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutSeconds)
		_, err := coord.etcdCli.Revoke(ctx, coord.leaseId)
		if err != nil {
			logger.Warning("Revoke err: %v", err)
		}
		coord.leaseId = clientv3.NoLease
		cancel()
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeoutSeconds)
	resp, err := coord.etcdCli.Grant(ctx, lib.MonitorLeaseTimeout)
	cancel()
	if err != nil {
		logger.Error("Grant err: %v", err)
		return err
	}

	keepAliveCtx := context.Background()
	if _, err := coord.etcdCli.KeepAlive(keepAliveCtx, resp.ID); err != nil {
		logger.Error("KeepAlive err: %v", err)
		coord.etcdCli.Revoke(keepAliveCtx, resp.ID)
		return err
	}

	ctx, cancel = context.WithTimeout(context.Background(), timeoutSeconds)
	key := fmt.Sprintf("%s/%s", coord.prefix, coord.id)
	_, err = coord.etcdCli.Put(ctx, key, coord.id, clientv3.WithLease(resp.ID))
	cancel()
	if err != nil {
		logger.Error("Put err: %v", err)
		coord.etcdCli.Revoke(ctx, resp.ID)
		return err
	}

	coord.leaseId = resp.ID

	return nil
}

func (coord *coordinator) initWatch() {
	coord.mu.Lock()
	defer coord.mu.Unlock()

	ctx := context.Background()
	ch := coord.etcdCli.Watch(ctx, coord.prefix, clientv3.WithPrefix())
	coord.wg.Add(1)
	go func() {
		defer coord.wg.Done()
		for {
			select {
			case <-ch:
				logger.Info("Coordinator update")
				coord.updateTotalAndCurrent()
			case <-coord.quit:
				logger.Info("Coordinator Watch exit")
				return
			}
		}
	}()
}

func newCoordinator(etcdCli *clientv3.Client, kf *lib.KeyFmt) *coordinator {
	coord := &coordinator{
		etcdCli: etcdCli,
		prefix:  kf.MonitorPrefix(),
		id:      uuid.New().String(),
		leaseId: clientv3.NoLease,
		total:   0,
		current: -1,
		quit:    make(chan bool),
	}
	coord.initLease()
	coord.initWatch()
	coord.updateTotalAndCurrent()
	return coord
}
