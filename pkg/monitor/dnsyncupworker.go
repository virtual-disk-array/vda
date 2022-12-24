package monitor

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
)

type dnSyncupWorker struct {
	name      string
	kf        *lib.KeyFmt
	sm        *lib.SyncupManager
	dnTimeout int
}

func (dsw *dnSyncupWorker) getName() string {
	return dsw.name
}

func (dsw *dnSyncupWorker) getRange(begin, end int) (string, string) {
	key := dsw.kf.DnErrWithHash(uint32(begin))
	endKey := dsw.kf.DnErrWithHash(uint32(end))
	return key, endKey
}

func (dsw *dnSyncupWorker) processBacklog(ctx context.Context, key string) {
	logger.Info("process key: %s %s", dsw.name, key)
	_, sockAddr, err := dsw.kf.DecodeDnErrKey(key)
	if err != nil {
		logger.Error("Decode key err: %s %v", dsw.name, err)
		return
	}
	dnCtx, cancel := context.WithTimeout(context.Background(),
		time.Duration(dsw.dnTimeout)*time.Second)
	dsw.sm.SyncupDn(sockAddr, dnCtx)
	cancel()
}

func newDnSyncupWorker(etcdCli *clientv3.Client, kf *lib.KeyFmt,
	dnTimeout int) *dnSyncupWorker {
	sw := lib.NewStmWrapper(etcdCli)
	sm := lib.NewSyncupManager(kf, sw)
	return &dnSyncupWorker{
		name:      "DnSyncupWorker",
		kf:        kf,
		sm:        sm,
		dnTimeout: dnTimeout,
	}
}
