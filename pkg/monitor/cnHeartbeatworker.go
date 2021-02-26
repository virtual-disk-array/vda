package monitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbcn "github.com/virtual-disk-array/vda/pkg/proto/cnagentapi"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
)

type cnHeartbeatWorker struct {
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
	cnTimeout        int
}

func (chw *cnHeartbeatWorker) getName() string {
	return chw.name
}

func (chw *cnHeartbeatWorker) getRange(begin, end int) (string, string) {
	key := chw.kf.CnListWithHash(uint32(begin))
	endKey := chw.kf.CnListWithHash(uint32(end))
	return key, endKey
}

func (chw *cnHeartbeatWorker) setErr(ctx context.Context, sockAddr string, errMsg string) {
	cnEntityKey := chw.kf.CnEntityKey(sockAddr)
	apply := func(stm concurrency.STM) error {
		controllerNode := &pbds.ControllerNode{}
		cnEntityVal := []byte(stm.Get(cnEntityKey))
		if len(cnEntityVal) == 0 {
			logger.Warning("Can not find controllerNode: %s %s", chw.name, sockAddr)
			return fmt.Errorf("Can not find controllerNode")
		}
		err := proto.Unmarshal(cnEntityVal, controllerNode)
		if err != nil {
			logger.Error("Unmarshal controllerNode err: %s %s %v", chw.name, sockAddr, err)
			return err
		}
		controllerNode.CnInfo.ErrInfo.IsErr = true
		controllerNode.CnInfo.ErrInfo.ErrMsg = errMsg
		controllerNode.CnInfo.ErrInfo.Timestamp = lib.ResTimestamp()
		cnErrKey := chw.kf.CnErrKey(controllerNode.CnConf.HashCode, controllerNode.SockAddr)
		cnSummary := &pbds.CnSummary{
			Description: controllerNode.CnConf.Description,
		}
		cnErrVal, err := proto.Marshal(cnSummary)
		if err != nil {
			logger.Error("Marshal cnSummary err: %s %v %v", chw.name, cnSummary, err)
			return err
		}
		cnErrValStr := string(cnErrVal)
		stm.Put(cnErrKey, cnErrValStr)

		// try to failover each primary cntlr
		for _, cntlrFe := range controllerNode.CntlrFeList {
			var thisCntlr pbds.Controller
			for _, cntlr := range cntlrFe.CntlrFeConf.CntlrList {
				if cntlr.CntlrId == cntlrFe.CntlrId {
					thisCntlr = cntlr
					break
				}
			}
			if thisCntlr == nil {
				logger.Error("Can not find thisCntlr: %s %v", chw.name, cntlrFe)
				return fmt.Errorf("Can not find thisCntlr")
			}
			if !thisCntlr.IsPrimary {
				continue
			}
			primaryGrpFeList := cntlrFe.GrpFeList
			primarySnapFeList := cntlrFe.SnapFeList
			primaryExpFeList := cntlrFe.ExpFeList

			var newPrimaryCntlr *pbds.Controller
			for _, cntlr := range cntlrFe.CntlrFeConf.CntlrList {
				if cntlr.CntlrId == cntlrFe.CntlrId {
					continue
				}
				controllerNode1 := &pbds.ControllerNode{}
				cnEntityKey1 := chw.kf.CnEntityKey(cntlr.CnSockAddr)
				cnEntityVal1 := []byte(stm.Get(cnEntityKey1))
				if len(cnEntityVal1) == 0 {
					logger.Warning("Can not find controllerNode1: %s %s",
						chw.name, cntlr.CnSockAddr)
					return fmt.Errorf("Can not find controllerNode1")
				}
				err := proto.Unmarshal(cnEntityVal1, controllerNode1)
				if err != nil {
					logger.Error("Unmarshal controllerNode1 err: %s %s %v",
						chw.name, cntlr.CnSockAddr, err)
					return fmt.Errorf("Unmarshal controllerNode1 err")
				}
				cnErrKey1 := chw.kf.CnErrKey(controllerNode1.CnConf.HashCode,
					controllerNode1.SockAddr)
				cnErrVal1 := []byte(stm.Get(cnErrKey1))
				if len(cnErrVal1) != 0 {
					continue
				}
				newPrimaryCntlr = cntlr
				break
			}
			if newPrimaryCntlr == nil {
				continue
			}
			diskArray := &pbds.DiskArray{}
			daEntityKey := chw.kf.DaEntityKey(cntlrFe.CntlrFeConf.DaName)
			daEntityVal := []byte(stm.Get(daEntityKey))
			if err := proto.Unmarshal(daEntityVal, diskArray); err != nil {
				logger.Error("Unmarshal diskArray err: %s %s %v",
					chw.name, daEntityKey, err)
				return err
			}
			for _, cntlr := range diskArray.CntlrList {
				if cntlr.CntlrId == newPrimaryCntlr.CntlrId {
					cntlr.IsPrimary = true
				} else {
					cntlr.IsPrimary = false
				}
				controllerNode2 := &pbds.ControllerNode{}
				cnEntityKey2 := chw.kf.CnEntityKey(cntlr.CnSockAddr)
				cnEntityVal2 := []byte(stm.Get(cnEntityKey2))
				if len(cnEntityVal2) == 0 {
					logger.Warning("Can not find controllerNode2: %s %s",
						chw.name, cntlr.CnSockAddr)
					return fmt.Errorf("Can not find controllerNode2")
				}
				for _, cntlrFe2 := range controllerNode2.CntlrFeList {
					if cntlrFe2.CntlrId == cntlr.CntlrId {
						for _, cntlr2 := range cntlrFe2.CntlrFeConf.CntlrList {
							if cntlr2.CntlrId == newPrimaryCntlr.CntlrId {
								cntlr2.IsPrimary = true
							} else {
								cntlr2.IsPrimary = false
							}
						}
						if cntlrFe2.CntlrId == newPrimaryCntlr {
							cntlrFe2.GrpFeList = primaryGrpFeList
							cntlrFe2.SnapFeList = primarySnapFeList
							cntlrFe2.ExpFeList = primaryExpFeList
						} else {
							cntlrFe2.GrpFeList = make([]*pbds.GrpFrontend, 0)
							cntlrFe2.SnapFeList = make([]*pbds.SnapFrontend, 0)
							cntlrFe2.ExpFeList = make([]*bpds.ExpFrontend, 0)
						}
						break
					}
				}
				newCnEntityVal2, err := proto.Marshal(controllerNode2)
				if err != nil {
					logger.Error("marshal controllerNode2 err: %s %v %v",
						chw.name, controllerNode2, err)
					return err
				}
				smt.Put(cnEntityKey2, newCnEntityVal2)
			}
			newDaEntityVal, err := proto.Marshal(diskArray)
			if err != nil {
				logger.Error("marshal diskArray err: %s %v %v", chw.name, diskArray, err)
				return err
			}
			stm.Put(daEntityKey, string(newDaEntityVal))
		}
		return nil
	}
	err := chw.sw.RunStm(apply, ctx, "setErr: "+sockAddr)
	if err != nil {
		logger.Error("RunStm err: %s %v", chw.name, err)
	}
}

func (chw *cnHeartbeatWorker) checkAndSetErr(ctx context.Context, sockAddr string, errMsg string) {
	now := time.Now().Unix()
	chw.mu.Lock()
	if now-chw.timestamp > chw.errBurstDuration {
		chw.timestamp = now
		chw.errCounter = 0
	}
	chw.errCounter++
	errCounter := chw.errCounter
	chw.mu.Unlock()
	if errCounter > chw.errBurstLimit {
		logger.Warning("errCounter is larger than errBurstLimit: %s %d %d",
			chw.name, errCounter, chw.errBurstLimit)
	} else {
		chw.setErr(ctx, sockAddr, errMsg)
	}
}

func (chw *cnHeartbeatWorker) processBacklog(ctx context.Context, key string) {
	logger.Info("process key: %s %s", chw.name, key)
	_, sockAddr, err := chw.kf.DecodeCnListKey(key)
	if err != nil {
		logger.Error("Decode key err: %s %v", chw.name, err)
		return
	}
	var revision int64
	cnEntityKey := chw.kf.CnEntityKey(sockAddr)
	apply := func(stm concurrency.STM) error {
		revision = stm.Rev(cnEntityKey)
		return nil
	}
	stmName := "GetRevision: " + chw.name + " " + sockAddr
	if err := chw.sw.RunStm(apply, ctx, stmName); err != nil {
		logger.Error("%s err: %s", stmName, err)
		return
	}
	if revision == 0 {
		logger.Warning("ControllerNode revision is 0: %s %s", chw.name, key)
		return
	}
	conn, err := chw.gc.Get(sockAddr)
	if err != nil {
		logger.Error("get conn err: %s %s %v", chw.name, sockAddr, err)
		return
	}
	c := pbcn.NewCnAgentClient(conn)
	req := &pbcn.CnHeartbeatRequest{
		ReqId:    uuid.New().String(),
		Revision: revision,
	}
	cnCtx, cancel := context.WithTimeout(context.Background(),
		time.Duration(chw.cnTimeout)*time.Second)
	reply, err := c.CnHeartbeat(cnCtx, req)
	cancel()
	if err != nil {
		logger.Warning("CnHeartbeat err: %s %v", chw.name, err)
		chw.checkAndSetErr(ctx, sockAddr, err.Error())
	} else {
		if reply.ReplyInfo.ReplyCode != lib.CnSucceedCode {
			logger.Warning("CnHeartbeat reply err; %s %v", chw.name, reply.ReplyInfo)
			chw.checkAndSetErr(ctx, sockAddr, reply.ReplyInfo.ReplyMsg)
		}
	}
}

func newCnHeartbeatWorker(etcdCli *clientv3.Client, kf *lib.KeyFmt,
	gc *lib.GrpcCache, errBurstLimit uint64, errBurstDuration int64,
	cnTimeout int) *cnHeartbeatWorker {
	return &cnHeartbeatWorker{
		name:             "CnHeartbeatWorker",
		etcdCli:          etcdCli,
		kf:               kf,
		gc:               gc,
		sw:               lib.NewStmWrapper(etcdCli),
		errCounter:       0,
		errBurstLimit:    errBurstLimit,
		timestamp:        time.Now().Unix(),
		errBurstDuration: errBurstDuration,
		cnTimeout:        cnTimeout,
	}
}
