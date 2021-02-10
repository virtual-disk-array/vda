package monitor

import (
	"github.com/virtual-disk-array/vda/pkg/logger"
)

type dnHeartbeatWorker struct {
	name          string
	backlogPrefix string
}

func (dhw *dnHeartbeatWorker) getName() string {
	return dhw.name
}

func (dhw *dnHeartbeatWorker) getBacklogPrefix() string {
	return dhw.backlogPrefix
}

func (dhw *dnHeartbeatWorker) processBacklog(key string) {
	logger.Info("process key: %s", key)
}

func newDnHeartbeatWorker() *dnHeartbeatWorker {
	return &dnHeartbeatWorker{
		name:          "DnHeartbeatWorker",
		backlogPrefix: "/a/b/d",
	}
}
