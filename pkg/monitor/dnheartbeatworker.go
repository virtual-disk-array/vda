package monitor

import (
	"context"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
)

type dnHeartbeatWorker struct {
	name string
	kf   *lib.KeyFmt
}

func (dhw *dnHeartbeatWorker) getName() string {
	return dhw.name
}

func (dhw *dnHeartbeatWorker) getRange(begin, end int) (string, string) {
	key := dhw.kf.DnListWithHash(uint32(begin))
	endKey := dhw.kf.DnListWithHash(uint32(end))
	return key, endKey
}

func (dhw *dnHeartbeatWorker) processBacklog(ctx context.Context, key string) {
	logger.Info("process key: %s", key)
}

func newDnHeartbeatWorker(kf *lib.KeyFmt) *dnHeartbeatWorker {
	return &dnHeartbeatWorker{
		name: "DnHeartbeatWorker",
		kf:   kf,
	}
}
