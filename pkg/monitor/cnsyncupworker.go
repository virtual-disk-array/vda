package monitor

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
)

type cnSyncupWorker struct {
	name      string
	kf        *lib.KeyFmt
	sm        *lib.SyncupManager
	cnTimeout int
}

func (csw *cnSyncupWorker) getName() string {
	return csw.name
}

func (csw *cnSyncupWorker) getRange(begin, end int) (string, string) {
	key := csw.kf.CnErrWithHash(uint32(begin))
	endKey := csw.kf.CnErrWithHash(uint32(end))
	return key, endKey
}

func (csw *cnSyncupWorker) processBacklog(ctx context.Context, key string) {
	logger.Info("process key: %s %s", csw.name, key)
	_, sockAddr, err := csw.kf.DecodeCnErrKey(key)
	if err != nil {
		logger.Error("Decode key err: %s %v", csw.name, err)
		return
	}
	cnCtx, cancel := context.WithTimeout(context.Background(),
		time.Duration(csw.cnTimeout)*time.Second)
	csw.sm.SyncupCn(sockAddr, cnCtx)
	cancel()
}

func newCnSyncupWorker(etcdCli *clientv3.Client, kf *lib.KeyFmt,
	cnTimeout int) *cnSyncupWorker {
	sw := lib.NewStmWrapper(etcdCli)
	sm := lib.NewSyncupManager(kf, sw)
	return &cnSyncupWorker{
		name:      "CnSyncupWorker",
		kf:        kf,
		sm:        sm,
		cnTimeout: cnTimeout,
	}
}
