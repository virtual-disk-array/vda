package monitor

import (
	"context"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
	pbdn "github.com/virtual-disk-array/vda/pkg/proto/dnagentapi"
)

type dnHeartbeatWorker struct {
	name             string
	etcdCli          *clientv3.Client
	kf               *lib.KeyFmt
	gc               *lib.GrpcCache
	sw               *lib.StmWrapper
	mu               sync.Mutex
	errCounter       uint64
	errBurstLimit    uint64
	timestamp        int64
	errBurstDuration int64
	dnTimeout        int
}

func (dhw *dnHeartbeatWorker) getName() string {
	return dhw.name
}

func (dhw *dnHeartbeatWorker) getRange(begin, end int) (string, string) {
	key := dhw.kf.DnListWithHash(uint32(begin))
	endKey := dhw.kf.DnListWithHash(uint32(end))
	return key, endKey
}

func (dhw *dnHeartbeatWorker) setErr(ctx context.Context, sockAddr string) {
	dnEntityKey := dhw.kf.DnEntityKey(sockAddr)
	apply := func(stm concurrency.STM) error {
		diskNode := &pbds.DiskNode{}
		dnEntityVal := []byte(stm.Get(dnEntityKey))
		if len(dnEntityVal) == 0 {
			logger.Warning("Can not find diskNode: %s", sockAddr)
			return nil
		}
		err := proto.Unmarshal(dnEntityVal, diskNode)
		if err != nil {
			logger.Error("Unmarshal diskNode err: %s %v", sockAddr, err)
			return nil
		}
		dnErrKey := dhw.kf.DnErrKey(diskNode.DnConf.HashCode, diskNode.SockAddr)
		dnSummary := &pbds.DnSummary{
			Description: diskNode.DnConf.Description,
		}
		dnErrVal, err := proto.Marshal(dnSummary)
		if err != nil {
			logger.Error("Marshal dnSummary err: %v %v", dnSummary, err)
			return nil
		}
		dnErrValStr := string(dnErrVal)
		stm.Put(dnErrKey, dnErrValStr)
		return nil
	}
	err := dhw.sw.RunStm(apply, ctx, "setErr: "+sockAddr)
	if err != nil {
		logger.Error("RunStm err: %v", err)
	}
}

func (dhw *dnHeartbeatWorker) checkAndsetErr(ctx context.Context, sockAddr string) {
	now := time.Now().Unix()
	dhw.mu.Lock()
	if now-dhw.timestamp > dhw.errBurstDuration {
		dhw.timestamp = now
		dhw.errCounter = 0
	}
	dhw.errCounter++
	errCounter := dhw.errCounter
	dhw.mu.Unlock()
	if errCounter > dhw.errBurstLimit {
		logger.Warning("errCounter is larger than errBurstLimit: %s %d %d",
			dhw.name, errCounter, dhw.errBurstLimit)
	} else {
		dhw.setErr(ctx, sockAddr)
	}
}

func (dhw *dnHeartbeatWorker) processBacklog(ctx context.Context, key string) {
	logger.Info("process key: %s", key)
	_, sockAddr, err := dhw.kf.DecodeDnListKey(key)
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
	if revision == 0 {
		logger.Warning("DiskNode revision is 0: %s", key)
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
	dnCtx, cancel := context.WithTimeout(context.Background(),
		time.Duration(dhw.dnTimeout)*time.Second)
	reply, err := c.DnHeartbeat(dnCtx, req)
	cancel()
	if err != nil {
		logger.Warning("DnHeartbeat err: %v", err)
		dhw.checkAndsetErr(ctx, sockAddr)
	} else {
		if reply.ReplyInfo.ReplyCode != lib.DnSucceedCode {
			logger.Warning("DnHeartbeat reply err: %v", reply.ReplyInfo)
			dhw.checkAndsetErr(ctx, sockAddr)
		}
	}
}

func newDnHeartbeatWorker(etcdCli *clientv3.Client, kf *lib.KeyFmt,
	gc *lib.GrpcCache, errBurstLimit uint64, errBurstDuration int64,
	dnTimeout int) *dnHeartbeatWorker {
	return &dnHeartbeatWorker{
		name:             "DnHeartbeatWorker",
		etcdCli:          etcdCli,
		kf:               kf,
		gc:               gc,
		sw:               lib.NewStmWrapper(etcdCli),
		errCounter:       0,
		errBurstLimit:    errBurstLimit,
		timestamp:        time.Now().Unix(),
		errBurstDuration: errBurstDuration,
		dnTimeout:        dnTimeout,
	}
}
