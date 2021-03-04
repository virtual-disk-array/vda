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

func (chw *cnHeartbeatWorker) setErr(ctx context.Context,
	sockAddr string, errMsg string, failover bool) {
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

		if !failover {
			return nil
		}

		// try to failover each primary cntlr
		for _, cntlrFe := range controllerNode.CntlrFeList {
			var thisCntlr *pbds.Controller
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
					logger.Error("Can not find controllerNode1: %s %s",
						chw.name, cntlr.CnSockAddr)
					return fmt.Errorf("Can not find controllerNode1")
				}
				if err := proto.Unmarshal(cnEntityVal1, controllerNode1); err != nil {
					logger.Error("Unmarshal controllerNode1 err: %s %s %v",
						chw.name, cntlr.CnSockAddr, err)
					return fmt.Errorf("Unmarshal controllerNode1 err")
				}
				if controllerNode1.CnInfo.ErrInfo.IsErr {
					continue
				}
				newPrimaryCntlr = cntlr
				break
			}
			if newPrimaryCntlr == nil {
				logger.Warning("Can not failover: %s %s",
					chw.name, cntlrFe.CntlrFeConf.DaName)
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
					logger.Error("Can not find controllerNode2: %s %s",
						chw.name, cntlr.CnSockAddr)
					return fmt.Errorf("Can not find controllerNode2")
				}
				for _, cntlrFe2 := range controllerNode2.CntlrFeList {
					if cntlrFe.CntlrId == cntlr.CntlrId {
						for _, cntlr2 := range cntlrFe2.CntlrFeConf.CntlrList {
							if cntlr2.CntlrId == newPrimaryCntlr.CntlrId {
								cntlr2.IsPrimary = true
							} else {
								cntlr2.IsPrimary = false
							}
						}
						if cntlrFe2.CntlrId == newPrimaryCntlr.CntlrId {
							cntlrFe2.GrpFeList = primaryGrpFeList
							cntlrFe2.SnapFeList = primarySnapFeList
							cntlrFe2.ExpFeList = primaryExpFeList
						} else {
							cntlrFe2.GrpFeList = make([]*pbds.GrpFrontend, 0)
							cntlrFe2.SnapFeList = make([]*pbds.SnapFrontend, 0)
							cntlrFe2.ExpFeList = make([]*pbds.ExpFrontend, 0)
						}
						break
					}
				}
				newCnEntityVal2, err := proto.Marshal(controllerNode2)
				if err != nil {
					logger.Error("Marshal controllerNode2 err: %s %v %v",
						chw.name, controllerNode2, err)
					return err
				}
				stm.Put(cnEntityKey2, string(newCnEntityVal2))
				cnErrKey2 := chw.kf.CnErrKey(controllerNode2.CnConf.HashCode,
					controllerNode2.SockAddr)
				if (len(stm.Get(cnErrKey2))) == 0 {
					cnSummary2 := &pbds.CnSummary{
						Description: controllerNode2.CnConf.Description,
					}
					cnErrVal2, err := proto.Marshal(cnSummary2)
					if err != nil {
						logger.Error("Marshal cnSummary2 err: %s %v %v",
							chw.name, cnSummary2, err)
						return err
					}
					stm.Put(cnErrKey2, string(cnErrVal2))
				}

				for _, grp := range diskArray.GrpList {
					for _, vd := range grp.VdList {
						diskNode := &pbds.DiskNode{}
						dnEntityKey := chw.kf.DnEntityKey(vd.DnSockAddr)
						dnEntityVal := []byte(stm.Get(dnEntityKey))
						if len(dnEntityVal) == 0 {
							logger.Error("Can not find diskNode: %s %v",
								chw.name, vd.DnSockAddr)
							return fmt.Errorf("Can not find diskNode")
						}
						if err := proto.Unmarshal(dnEntityVal, diskNode); err != nil {
							logger.Error("Unmarshal diskNode err: %s %s %v",
								chw.name, vd.DnSockAddr, err)
							return err
						}
						var thisPd *pbds.PhysicalDisk
						for _, pd := range diskNode.PdList {
							if pd.PdName == vd.PdName {
								thisPd = pd
								break
							}
						}
						if thisPd == nil {
							logger.Error("Can not find pd: %s %s %v",
								chw.name, vd.PdName, diskNode)
							return fmt.Errorf("Can not find pd")
						}
						var thisVdBe *pbds.VdBackend
						for _, vdBe := range thisPd.VdBeList {
							if vdBe.VdId == vd.VdId {
								thisVdBe = vdBe
								break
							}
						}
						if thisVdBe == nil {
							logger.Error("Can not find vdBe: %s %s %v",
								chw.name, vd.VdId, diskNode)
							return fmt.Errorf("Can not find vdBe")
						}
						thisVdBe.VdBeConf.CntlrId = newPrimaryCntlr.CntlrId
						newDnEntityVal, err := proto.Marshal(diskNode)
						if err != nil {
							logger.Error("Marshal diskNode err: %s %v %v",
								chw.name, diskNode, err)
							return err
						}
						stm.Put(dnEntityKey, string(newDnEntityVal))
					}
				}

				newDaEntityVal, err := proto.Marshal(diskArray)
				if err != nil {
					logger.Error("Marshal diskArray err: %s %v %v",
						chw.name, diskArray, err)
					return err
				}
				stm.Put(daEntityKey, string(newDaEntityVal))
			}
		}
		return nil
	}
	err := chw.sw.RunStm(apply, ctx, "Cn setErr: "+sockAddr)
	if err != nil {
		logger.Error("RunStm err: %s %v", chw.name, err)
	}
}

func (chw *cnHeartbeatWorker) checkAndSetErr(ctx context.Context,
	sockAddr string, errMsg string, failover bool) {
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
		chw.setErr(ctx, sockAddr, errMsg, failover)
	}
}

func (chw *cnHeartbeatWorker) processBacklog(ctx context.Context, key string) {
	logger.Info("process key: %s %s", chw.name, key)
	_, sockAddr, err := chw.kf.DecodeCnListKey(key)
	if err != nil {
		logger.Error("Decode key err: %s %v", chw.name, err)
		return
	}
	var version uint64
	cnEntityKey := chw.kf.CnEntityKey(sockAddr)
	apply := func(stm concurrency.STM) error {
		val := []byte(stm.Get(cnEntityKey))
		if len(val) == 0 {
			logger.Error("Can not find cnEntityKey: %s %s", chw.name, cnEntityKey)
			return fmt.Errorf("Can not find cnEntityKey")
		}
		controllerNode := &pbds.ControllerNode{}
		err := proto.Unmarshal(val, controllerNode)
		if err != nil {
			logger.Error("Unmarshal controllerNode err: %s %v", chw.name, err)
			return err
		}
		version = controllerNode.Version
		return nil
	}
	stmName := "GetRevision: " + chw.name + " " + sockAddr
	if err := chw.sw.RunStm(apply, ctx, stmName); err != nil {
		logger.Error("%s err: %s", stmName, err)
		return
	}
	if version == 0 {
		logger.Warning("ControllerNode version is 0: %s %s", chw.name, key)
		return
	}
	conn, err := chw.gc.Get(sockAddr)
	if err != nil {
		logger.Error("get conn err: %s %s %v", chw.name, sockAddr, err)
		return
	}
	c := pbcn.NewCnAgentClient(conn)
	req := &pbcn.CnHeartbeatRequest{
		ReqId:   uuid.New().String(),
		Version: version,
	}
	cnCtx, cancel := context.WithTimeout(context.Background(),
		time.Duration(chw.cnTimeout)*time.Second)
	reply, err := c.CnHeartbeat(cnCtx, req)
	cancel()
	if err != nil {
		logger.Warning("CnHeartbeat err: %s %v", chw.name, err)
		chw.checkAndSetErr(ctx, sockAddr, err.Error(), true)
	} else {
		if reply.ReplyInfo.ReplyCode != lib.CnSucceedCode {
			logger.Warning("CnHeartbeat reply err; %s %v", chw.name, reply.ReplyInfo)
			chw.checkAndSetErr(ctx, sockAddr, reply.ReplyInfo.ReplyMsg, false)
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
