package monitor

import (
	"context"
	"fmt"
	// "os"
	// "os/signal"
	// "strings"
	"sync"
	// "syscall"
	// "time"

	"github.com/coreos/etcd/clientv3"
	"github.com/google/uuid"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
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
	coord.mu.Lock()
	defer coord.mu.Unlock()
	ctx := context.Background()
	if coord.leaseId != clientv3.NoLease {
		_, err := coord.etcdCli.Revoke(ctx, coord.leaseId)
		if err != nil {
			logger.Warning("Revoke err: %v", err)
		}
	}
	close(coord.quit)
	coord.wg.Wait()
}

func (coord *coordinator) initLease() error {
	coord.mu.Lock()
	defer coord.mu.Unlock()

	ctx := context.Background()

	if coord.leaseId != clientv3.NoLease {
		_, err := coord.etcdCli.Revoke(ctx, coord.leaseId)
		if err != nil {
			logger.Warning("Revoke err: %v", err)
		}
		coord.leaseId = clientv3.NoLease
	}

	resp, err := coord.etcdCli.Grant(ctx, lib.MonitorLeaseTimeout)
	if err != nil {
		logger.Error("Grant err: %v", err)
		return err
	}

	if _, err := coord.etcdCli.KeepAlive(ctx, resp.ID); err != nil {
		logger.Error("KeepAlive err: %v", err)
		coord.etcdCli.Revoke(ctx, resp.ID)
		return err
	}

	key := fmt.Sprintf("%s/%s", coord.prefix, coord.id)
	if _, err := coord.etcdCli.Put(ctx, key, coord.id, clientv3.WithLease(resp.ID)); err != nil {
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
		select {
		case <-ch:
			logger.Info("Coordiantor update")
			// logger.Info("Coordiantor update: %v", wrsep)
			// coord.UpdateTotalAndCurrent()
		case <-coord.quit:
			logger.Info("Coordinator Watch exit")
			return
		}
	}()
}

func newCoordinator(etcdCli *clientv3.Client, prefix string) *coordinator {
	coord := &coordinator{
		etcdCli: etcdCli,
		prefix:  prefix,
		id:      uuid.New().String(),
		leaseId: clientv3.NoLease,
		total:   0,
		current: -1,
		quit:    make(chan bool),
	}
	coord.initLease()
	coord.initWatch()
	return coord
}
