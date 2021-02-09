package monitor

import (
	"github.com/virtual-disk-array/vda/pkg/logger"
)

type dnHeartbeatWorker struct {
	name          string
	regPath       string
	backlogPrefix string
}

func (dhw *dnHeartbeatWorker) getName() string {
	return dhw.name
}

func (dhw *dnHeartbeatWorker) getRegPath() string {
	return dhw.regPath
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
		regPath:       "/a/b/c",
		backlogPrefix: "/a/b/d",
	}
}
