package monitor

import (
	"context"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/google/uuid"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbdn "github.com/virtual-disk-array/vda/pkg/proto/dnagentapi"
)

type dnHeartbeatWorker struct {
	name       string
	etcdCli    *clientv3.Client
	kf         *lib.KeyFmt
	gc         *lib.GrpcCache
	mu         sync.Mutex
	errCounter uint64
	threshold  uint64
	timestamp  int64
	interval   int64
	sw         *lib.StmWrapper
}

func (dhw *dnHeartbeatWorker) getName() string {
	return dhw.name
}

func (dhw *dnHeartbeatWorker) getRange(begin, end int) (string, string) {
	key := dhw.kf.DnListWithHash(uint32(begin))
	endKey := dhw.kf.DnListWithHash(uint32(end))
	return key, endKey
}

func (dhw *dnHeartbeatWorker) setErr(ctx context.Context, sockAddr string, hashCode uint32) {
}

func (dhw *dnHeartbeatWorker) checkAndsetErr(ctx context.Context, sockAddr string, hashCode uint32) {
	now := time.Now().Unix()
	dhw.mu.Lock()
	if now-dhw.timestamp > dhw.interval {
		dhw.timestamp = now
		dhw.errCounter = 0
	}
	dhw.errCounter++
	errCounter := dhw.errCounter
	dhw.mu.Unlock()
	if errCounter > dhw.threshold {
		logger.Warning("errCounter is larger than threshold: %s %d %d",
			dhw.name, errCounter, dhw.threshold)
	} else {
		dhw.setErr(ctx, sockAddr, hashCode)
	}
}

func (dhw *dnHeartbeatWorker) processBacklog(ctx context.Context, key string) {
	logger.Info("process key: %s", key)
	hashCode, sockAddr, err := dhw.kf.DecodeDnListKey(key)
	if err != nil {
		logger.Error("Decode key err: %v", err)
		return
	}
	var revision int64
	dnEntityKey := dhw.kf.DnEntityKey(sockAddr)
	apply := func(stm concurrency.STM) error {
		revision = stm.Rev(dnEntityKey)
		return nil
	}
	stmName := "GetRevision: " + dhw.name + " " + sockAddr
	if err := dhw.sw.RunStm(apply, ctx, stmName); err != nil {
		logger.Error("%s err: %s", stmName, err)
		return
	}
	conn, err := dhw.gc.Get(sockAddr)
	if err != nil {
		logger.Error("get conn err: %s %v", sockAddr, err)
		return
	}
	c := pbdn.NewDnAgentClient(conn)
	req := &pbdn.DnHeartbeatRequest{
		ReqId:    uuid.New().String(),
		Revision: revision,
	}
	reply, err := c.DnHeartbeat(ctx, req)
	if err != nil {
		// if ser, ok := err.(context.DeadlineExceeded); ok {
		// 	logger.Error("DeadlineExceeded: %s", dhw.name)
		// } else {
		logger.Warning("DnHeartbeat err: %v", err)
		dhw.checkAndsetErr(ctx, sockAddr, hashCode)
		// }
	} else {
		if reply.ReplyInfo.ReplyCode != lib.DnSucceedCode {
			logger.Warning("DnHeartbeat reply err: %v", reply.ReplyInfo)
			dhw.checkAndsetErr(ctx, sockAddr, hashCode)
		}
	}
}

func newDnHeartbeatWorker(etcdCli *clientv3.Client, kf *lib.KeyFmt,
	gc *lib.GrpcCache) *dnHeartbeatWorker {
	return &dnHeartbeatWorker{
		name:       "DnHeartbeatWorker",
		etcdCli:    etcdCli,
		kf:         kf,
		gc:         gc,
		errCounter: 0,
		threshold:  10,
		timestamp:  time.Now().Unix(),
		interval:   10,
	}
}
