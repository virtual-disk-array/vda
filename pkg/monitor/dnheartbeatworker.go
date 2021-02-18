package monitor

import (
	"context"

	"github.com/coreos/etcd/clientv3"
	"github.com/google/uuid"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbdn "github.com/virtual-disk-array/vda/pkg/proto/dnagentapi"
)

type dnHeartbeatWorker struct {
	name    string
	etcdCli *clientv3.Client
	kf      *lib.KeyFmt
	gc      *lib.GrpcCache
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
	_, sockAddr, err := dhw.kf.DecodeDnListKey(key)
	if err != nil {
		logger.Error("Decode key err: %v", err)
		return
	}
	conn, err := dhw.gc.Get(sockAddr)
	if err != nil {
		logger.Warning("get conn err: %s %v", sockAddr, err)
		return
	}
	c := pbdn.NewDnAgentClient(conn)
	req := &pbdn.DnHeartbeatRequest{
		ReqId: uuid.New().String(),
	}
	reply, err := c.DnHeartbeat(ctx, req)
	logger.Info("reply: %v", reply)
	logger.Info("err: %v", err)
}

func newDnHeartbeatWorker(etcdCli *clientv3.Client, kf *lib.KeyFmt,
	gc *lib.GrpcCache) *dnHeartbeatWorker {
	return &dnHeartbeatWorker{
		name:    "DnHeartbeatWorker",
		etcdCli: etcdCli,
		kf:      kf,
		gc:      gc,
	}
}
