package portal

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/protobuf/proto"
	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

func (po *portalServer) CreateSnap(ctx context.Context, req *pbpo.CreateSnapRequest) (*pbpo.CreateSnapReply, error) {
	invalidParamMsg := ""
	if req.DaName == "" {
		invalidParamMsg = "DaName is empty"
	} else if req.SnapName == "" {
		invalidParamMsg = "SnapName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.CreateSnapReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	daEntityKey := po.kf.DaEntityKey(req.DaName)
	diskArray := &pbds.DiskArray{}

	var primarySockAddr string
	var newSnap *pbds.Snap

	apply := func(stm concurrency.STM) error {
		dnEntityVal := []byte(stm.Get(daEntityKey))
		if len(dnEntityVal) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				daEntityKey,
			}
		}
		if err := proto.Unmarshal(dnEntityVal, diskArray); err != nil {
			logger.Error("Unmarshal diskArray err: %s %v", daEntityKey, err)
			return err
		}
		snapMap := make(map[string]*pbds.Snap)
		for _, snap := range diskArray.SnapList {
			snapMap[snap.SnapName] = snap
		}

		if _, ok := snapMap[req.OriName]; req.OriName != "" && !ok {
			invalidParamMsg = "Snap is not existed with given OriName"
		} else if _, ok := snapMap[""]; req.OriName == "" && ok {
			invalidParamMsg = "da has snap without SnapName"
		} else if _, ok := snapMap[req.OriName]; req.OriName != "" && ok && req.IsClone && snapMap[req.OriName].IsClone {
			invalidParamMsg = "Clones may be created only from snapshots"
		}

		if invalidParamMsg != "" {
			return fmt.Errorf("request parameter err: %v", invalidParamMsg)
		}

		var oriSnap *pbds.Snap
		if req.OriName == "" {
			if _, ok := snapMap[lib.DefaultSanpName]; ok {
				oriSnap = snapMap[lib.DefaultSanpName]
			} else {
				return fmt.Errorf("default snap is not existed")
			}
		} else {
			oriSnap = snapMap[req.OriName]
		}

		var newSnapSize uint64
		if req.Size == 0 {
			newSnapSize = oriSnap.Size
		} else {
			newSnapSize = req.Size
		}

		if newSnapSize < oriSnap.Size {
			return fmt.Errorf("request parameter err: req.Size should greater than oriSnap Size %d", oriSnap.Size)
		}

		newSnap = &pbds.Snap{
			SnapId:      lib.NewHexStrUuid(),
			SnapName:    req.SnapName,
			Description: req.Description,
			OriName:     oriSnap.SnapName,
			IsClone:     req.IsClone,
			Idx:         uint64(stm.Rev(daEntityKey)),
			Size:        newSnapSize,
		}

		diskArray.SnapList = append(diskArray.SnapList, newSnap)

		newDaEntityVal, err := proto.Marshal(diskArray)
		if err != nil {
			logger.Error("Marshal diskArray err: %v %v", diskArray, err)
			return fmt.Errorf("marshal diskArray err: %v", err)
		}

		stm.Put(daEntityKey, string(newDaEntityVal))

		for _, cntlr := range diskArray.CntlrList {
			if cntlr.IsPrimary {
				controllerNode := &pbds.ControllerNode{}
				cnEntityKey := po.kf.CnEntityKey(cntlr.CnSockAddr)
				cnEntityVal := []byte(stm.Get(cnEntityKey))
				if err := proto.Unmarshal(cnEntityVal, controllerNode); err != nil {
					logger.Error("Unmarshal controllerNode err: %s %v", cnEntityKey, err)
					return err
				}
				primarySockAddr = controllerNode.SockAddr
				snapFrontend := &pbds.SnapFrontend{
					SnapId: newSnap.SnapId,
					SnapFeConf: &pbds.SnapFeConf{
						OriId:   oriSnap.SnapId,
						IsClone: newSnap.IsClone,
						Idx:     newSnap.Idx,
						Size:    newSnap.Size,
					},
					SnapFeInfo: &pbds.SnapFeInfo{
						ErrInfo: &pbds.ErrInfo{
							IsErr:     true,
							ErrMsg:    lib.ResUninitMsg,
							Timestamp: lib.ResTimestamp(),
						},
					},
				}
				for _, cntlrFe := range controllerNode.CntlrFeList {
					cntlrFe.SnapFeList = append(cntlrFe.SnapFeList, snapFrontend)
				}
				controllerNode.Version++
				newCnEntityVal, err := proto.Marshal(controllerNode)
				if err != nil {
					logger.Error("Marshal controllerNode err: %v %v", controllerNode, err)
					return fmt.Errorf("marshal controllerNode err: %v", err)
				}
				stm.Put(cnEntityKey, string(newCnEntityVal))
			}
		}
		return nil
	}
	err := po.sw.RunStm(apply, ctx, "CreateSnap: "+req.DaName+" "+req.SnapName)

	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.CreateSnapReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.CreateSnapReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	}

	po.sm.SyncupCn(primarySockAddr, ctx)

	return &pbpo.CreateSnapReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}
