package portal

import (
	"context"
	"fmt"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/golang/protobuf/proto"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

func (po *portalServer) CreatePd(ctx context.Context, req *pbpo.CreatePdRequest) (
	*pbpo.CreatePdReply, error) {
	invalidParamMsg := ""
	if req.SockAddr == "" {
		invalidParamMsg = "SockAddr is empty"
	} else if req.PdName == "" {
		invalidParamMsg = "PdName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.CreatePdReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	pdId := lib.NewHexStrUuid()
	pdConf := &pbds.PdConf{}
	pdConf.Description = req.Description
	pdConf.IsOffline = req.IsOffline

	switch x := req.BdevType.(type) {
	case *pbpo.CreatePdRequest_BdevMalloc:
		pdConf.BdevType = &pbds.PdConf_BdevMalloc{
			BdevMalloc: &pbds.BdevMalloc{
				Size: x.BdevMalloc.Size,
			},
		}
	case *pbpo.CreatePdRequest_BdevAio:
		pdConf.BdevType = &pbds.PdConf_BdevAio{
			BdevAio: &pbds.BdevAio{
				FileName: x.BdevAio.FileName,
			},
		}
	case *pbpo.CreatePdRequest_BdevNvme:
		pdConf.BdevType = &pbds.PdConf_BdevNvme{
			BdevNvme: &pbds.BdevNvme{
				TrAddr: x.BdevNvme.TrAddr,
			},
		}
	default:
		logger.Error("Unknow BdevType: %v", x)
		return &pbpo.CreatePdReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  "Unknow BdevType",
			},
		}, nil
	}

	pdInfo := &pbds.PdInfo{
		ErrInfo: &pbds.ErrInfo{
			IsErr:     true,
			ErrMsg:    lib.ResUninitMsg,
			Timestamp: lib.ResTimestamp(),
		},
	}

	cap := &pbds.PdCapacity{
		TotalSize: uint64(0),
		FreeSize:  uint64(0),
		TotalQos: &pbds.BdevQos{
			RwIosPerSec:    req.Qos.RwIosPerSec,
			RwMbytesPerSec: req.Qos.RwMbytesPerSec,
			RMbytesPerSec:  req.Qos.RMbytesPerSec,
			WMbytesPerSec:  req.Qos.WMbytesPerSec,
		},
		FreeQos: &pbds.BdevQos{
			RwIosPerSec:    req.Qos.RwIosPerSec,
			RwMbytesPerSec: req.Qos.RwMbytesPerSec,
			RMbytesPerSec:  req.Qos.RMbytesPerSec,
			WMbytesPerSec:  req.Qos.WMbytesPerSec,
		},
	}

	vdBeList := make([]*pbds.VdBackend, 0)

	physicalDisk := &pbds.PhysicalDisk{
		PdId:     pdId,
		PdName:   req.PdName,
		PdConf:   pdConf,
		PdInfo:   pdInfo,
		Capacity: cap,
		VdBeList: vdBeList,
	}

	dnEntityKey := po.kf.DnEntityKey(req.SockAddr)
	diskNode := &pbds.DiskNode{}

	apply := func(stm concurrency.STM) error {
		val := []byte(stm.Get(dnEntityKey))
		if len(val) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				dnEntityKey,
			}
		}
		if err := proto.Unmarshal(val, diskNode); err != nil {
			logger.Error("Unmarshal diskNode err: %s %v", dnEntityKey, err)
			return err
		}

		if diskNode.SockAddr != req.SockAddr {
			logger.Error("SockAddr mismatch: %s %v", req.SockAddr, diskNode)
			return &portalError{
				lib.PortalInternalErrCode,
				"DiskNode SockAddr mismatch",
			}
		}
		for _, pd := range diskNode.PdList {
			if pd.PdName == physicalDisk.PdName {
				return &portalError{
					lib.PortalDupResErrCode,
					physicalDisk.PdName,
				}
			}
		}
		diskNode.PdList = append(diskNode.PdList, physicalDisk)
		dnEntityVal, err := proto.Marshal(diskNode)
		if err != nil {
			logger.Error("Marshal diskNode err: %v %v", diskNode, err)
			return err
		}
		dnEntityValStr := string(dnEntityVal)
		stm.Put(dnEntityKey, dnEntityValStr)

		dnErrKey := po.kf.DnErrKey(diskNode.DnConf.HashCode, diskNode.SockAddr)
		if len(stm.Get(dnErrKey)) == 0 {
			dnSummary := &pbds.DnSummary{
				SockAddr:    diskNode.SockAddr,
				Description: diskNode.DnConf.Description,
			}
			dnErrVal, err := proto.Marshal(dnSummary)
			if err != nil {
				logger.Error("marshal dnSummary err: %v %v", dnSummary, err)
				return err
			}
			dnErrValStr := string(dnErrVal)
			stm.Put(dnErrKey, dnErrValStr)
		}

		return nil
	}

	err := po.sw.RunStm(apply, ctx, "CreatePd: "+req.SockAddr+" "+req.PdName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.CreatePdReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.CreatePdReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	}

	po.sm.SyncupDn(req.SockAddr, ctx)

	return &pbpo.CreatePdReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}

func (po *portalServer) DeletePd(ctx context.Context, req *pbpo.DeletePdRequest) (
	*pbpo.DeletePdReply, error) {
	invalidParamMsg := ""
	if req.SockAddr == "" {
		invalidParamMsg = "SockAddr is empty"
	} else if req.PdName == "" {
		invalidParamMsg = "PdName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.DeletePdReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	dnEntityKey := po.kf.DnEntityKey(req.SockAddr)
	diskNode := &pbds.DiskNode{}

	apply := func(stm concurrency.STM) error {
		val := []byte(stm.Get(dnEntityKey))
		if len(val) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				dnEntityKey,
			}
		}
		if err := proto.Unmarshal(val, diskNode); err != nil {
			logger.Error("Unmarshal diskNode err: %s %v", dnEntityKey, err)
			return err
		}

		if diskNode.SockAddr != req.SockAddr {
			logger.Error("SockAddr mismatch: %s %v", req.SockAddr, diskNode)
			return &portalError{
				lib.PortalInternalErrCode,
				"DiskNode SockAddr mismatch",
			}
		}

		var targetPd *pbds.PhysicalDisk
		var targetIdx int
		for i, pd := range diskNode.PdList {
			if pd.PdName == req.PdName {
				targetPd = pd
				targetIdx = i
				break
			}
		}
		if targetPd == nil {
			return &portalError{
				lib.PortalUnknownResErrCode,
				req.PdName,
			}
		}
		if len(targetPd.VdBeList) > 0 {
			return &portalError{
				lib.PortalResBusyErrCode,
				fmt.Sprintf("VdBe cnt: %d", len(targetPd.VdBeList)),
			}
		}
		length := len(diskNode.PdList)
		diskNode.PdList[targetIdx] = diskNode.PdList[length-1]
		diskNode.PdList = diskNode.PdList[:length-1]
		dnEntityVal, err := proto.Marshal(diskNode)
		if err != nil {
			logger.Error("Marshal diskNode err: %v %v", diskNode, err)
			return err
		}
		dnEntityValStr := string(dnEntityVal)
		stm.Put(dnEntityKey, dnEntityValStr)
		cap := targetPd.Capacity
		dnCapKey := po.kf.DnCapKey(cap.FreeSize, diskNode.SockAddr, targetPd.PdName)
		stm.Del(dnCapKey)
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "DeletePd: "+req.SockAddr+" "+req.PdName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.DeletePdReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.DeletePdReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	}

	po.sm.SyncupDn(req.SockAddr, ctx)

	return &pbpo.DeletePdReply{
		ReplyInfo: &pbpo.ReplyInfo{
			ReqId:     lib.GetReqId(ctx),
			ReplyCode: lib.PortalSucceedCode,
			ReplyMsg:  lib.PortalSucceedMsg,
		},
	}, nil
}

func (po *portalServer) modifyPdDescription(ctx context.Context, sockAddr string,
	pdName string, description string) (*pbpo.ModifyPdReply, error) {
	dnEntityKey := po.kf.DnEntityKey(sockAddr)
	diskNode := &pbds.DiskNode{}

	apply := func(stm concurrency.STM) error {
		dnEntityVal := []byte(stm.Get(dnEntityKey))
		if len(dnEntityVal) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				dnEntityKey,
			}
		}
		if err := proto.Unmarshal(dnEntityVal, diskNode); err != nil {
			logger.Error("Unmarshal diskNode err: %s", dnEntityKey)
			return err
		}

		if diskNode.SockAddr != sockAddr {
			logger.Error("SockAddr mismatch: %s %v", sockAddr, diskNode)
			return &portalError{
				lib.PortalInternalErrCode,
				"DiskNode SockAddr mismatch",
			}
		}

		var targetPd *pbds.PhysicalDisk
		for _, pd := range diskNode.PdList {
			if pd.PdName == pdName {
				targetPd = pd
				break
			}
		}
		if targetPd == nil {
			return &portalError{
				lib.PortalUnknownResErrCode,
				pdName,
			}
		}
		targetPd.PdConf.Description = description
		dnEntityVal, err := proto.Marshal(diskNode)
		if err != nil {
			logger.Error("Marshal diskNode err: %v %v", diskNode, err)
			return err
		}
		dnEntityValStr := string(dnEntityVal)
		stm.Put(dnEntityKey, dnEntityValStr)
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "ModifyPdDescription: "+sockAddr+" "+pdName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.ModifyPdReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.ModifyPdReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.ModifyPdReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
		}, nil
	}
}

func (po *portalServer) modifyPdIsOffline(ctx context.Context, sockAddr string,
	pdName string, isOffline bool) (*pbpo.ModifyPdReply, error) {
	dnEntityKey := po.kf.DnEntityKey(sockAddr)
	diskNode := &pbds.DiskNode{}

	apply := func(stm concurrency.STM) error {
		dnEntityVal := []byte(stm.Get(dnEntityKey))
		if len(dnEntityVal) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				dnEntityKey,
			}
		}
		if err := proto.Unmarshal(dnEntityVal, diskNode); err != nil {
			logger.Error("Unmarshal diskNode err: %s", dnEntityKey)
			return err
		}

		if diskNode.SockAddr != sockAddr {
			logger.Error("SockAddr mismatch: %s %v", sockAddr, diskNode)
			return &portalError{
				lib.PortalInternalErrCode,
				"DiskNode SockAddr mismatch",
			}
		}

		var targetPd *pbds.PhysicalDisk
		for _, pd := range diskNode.PdList {
			if pd.PdName == pdName {
				targetPd = pd
				break
			}
		}
		if targetPd == nil {
			return &portalError{
				lib.PortalUnknownResErrCode,
				pdName,
			}
		}
		targetPd.PdConf.IsOffline = isOffline
		dnEntityVal, err := proto.Marshal(diskNode)
		if err != nil {
			logger.Error("Marshal diskNode err: %v %v", diskNode, err)
			return err
		}
		dnEntityValStr := string(dnEntityVal)
		stm.Put(dnEntityKey, dnEntityValStr)
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "ModifyPdIsOffline: "+sockAddr+" "+pdName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.ModifyPdReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.ModifyPdReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.ModifyPdReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
		}, nil
	}
}
func (po *portalServer) ModifyPd(ctx context.Context, req *pbpo.ModifyPdRequest) (
	*pbpo.ModifyPdReply, error) {
	invalidParamMsg := ""
	if req.SockAddr == "" {
		invalidParamMsg = "SockAddr is empty"
	} else if req.PdName == "" {
		invalidParamMsg = "PdName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.ModifyPdReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}
	switch x := req.Attr.(type) {
	case *pbpo.ModifyPdRequest_Description:
		return po.modifyPdDescription(ctx, req.SockAddr, req.PdName, x.Description)
	case *pbpo.ModifyPdRequest_IsOffline:
		return po.modifyPdIsOffline(ctx, req.SockAddr, req.PdName, x.IsOffline)
	default:
		logger.Error("Unknow attr: %v", x)
		return &pbpo.ModifyPdReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  "Unknow attr",
			},
		}, nil
	}
}

func (po *portalServer) ListPd(ctx context.Context, req *pbpo.ListPdRequest) (
	*pbpo.ListPdReply, error) {
	invalidParamMsg := ""
	if req.SockAddr == "" {
		invalidParamMsg = "SockAddr is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.ListPdReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	dnEntityKey := po.kf.DnEntityKey(req.SockAddr)
	diskNode := &pbds.DiskNode{}

	var pdSummaryList []*pbpo.PdSummary
	apply := func(stm concurrency.STM) error {
		val := []byte(stm.Get(dnEntityKey))
		if len(val) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				dnEntityKey,
			}
		}
		if err := proto.Unmarshal(val, diskNode); err != nil {
			logger.Error("Unmarshal diskNode err: %s %v", dnEntityKey, err)
			return err
		}

		if diskNode.SockAddr != req.SockAddr {
			logger.Error("SockAddr mismatch: %s %v", req.SockAddr, diskNode)
			return &portalError{
				lib.PortalInternalErrCode,
				"DiskNode SockAddr mismatch",
			}
		}

		pdSummaryList = make([]*pbpo.PdSummary, 0)
		for _, pd := range diskNode.PdList {
			pdSummary := &pbpo.PdSummary{
				PdName:      pd.PdName,
				Description: pd.PdConf.Description,
			}
			pdSummaryList = append(pdSummaryList, pdSummary)
		}
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "ListPd: "+req.SockAddr)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.ListPdReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.ListPdReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		return &pbpo.ListPdReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInternalErrCode,
				ReplyMsg:  err.Error(),
			},
			PdSummaryList: pdSummaryList,
		}, nil
	}
}

func (po *portalServer) GetPd(ctx context.Context, req *pbpo.GetPdRequest) (
	*pbpo.GetPdReply, error) {
	invalidParamMsg := ""
	if req.SockAddr == "" {
		invalidParamMsg = "SockAddr is empty"
	} else if req.PdName == "" {
		invalidParamMsg = "PdName is empty"
	}
	if invalidParamMsg != "" {
		return &pbpo.GetPdReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalInvalidParamCode,
				ReplyMsg:  invalidParamMsg,
			},
		}, nil
	}

	dnEntityKey := po.kf.DnEntityKey(req.SockAddr)
	diskNode := &pbds.DiskNode{}
	var targetPd *pbds.PhysicalDisk

	apply := func(stm concurrency.STM) error {
		val := []byte(stm.Get(dnEntityKey))
		if len(val) == 0 {
			return &portalError{
				lib.PortalUnknownResErrCode,
				dnEntityKey,
			}
		}
		if err := proto.Unmarshal(val, diskNode); err != nil {
			logger.Error("Unmarshal diskNode err: %s %v", dnEntityKey, err)
			return err
		}

		if diskNode.SockAddr != req.SockAddr {
			logger.Error("SockAddr mismatch: %s %v", req.SockAddr, diskNode)
			return &portalError{
				lib.PortalInternalErrCode,
				"DiskNode SockAddr mismatch",
			}
		}

		for _, pd := range diskNode.PdList {
			if pd.PdName == req.PdName {
				targetPd = pd
				break
			}
		}
		if targetPd == nil {
			return &portalError{
				lib.PortalUnknownResErrCode,
				req.PdName,
			}
		}
		return nil
	}

	err := po.sw.RunStm(apply, ctx, "GetPd: "+req.SockAddr+" "+req.PdName)
	if err != nil {
		if serr, ok := err.(*portalError); ok {
			return &pbpo.GetPdReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: serr.code,
					ReplyMsg:  serr.msg,
				},
			}, nil
		} else {
			return &pbpo.GetPdReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  err.Error(),
				},
			}, nil
		}
	} else {
		vdBeList := make([]*pbpo.VdBackend, 0)
		for _, dsVdBe := range targetPd.VdBeList {
			poVdBe := &pbpo.VdBackend{
				VdId:   dsVdBe.VdId,
				DaName: dsVdBe.VdBeConf.DaName,
				GrpIdx: dsVdBe.VdBeConf.GrpIdx,
				VdIdx:  dsVdBe.VdBeConf.VdIdx,
				Size:   dsVdBe.VdBeConf.Size,
				Qos: &pbpo.BdevQos{
					RwIosPerSec:    dsVdBe.VdBeConf.Qos.RwIosPerSec,
					RwMbytesPerSec: dsVdBe.VdBeConf.Qos.RwMbytesPerSec,
					RMbytesPerSec:  dsVdBe.VdBeConf.Qos.RMbytesPerSec,
					WMbytesPerSec:  dsVdBe.VdBeConf.Qos.WMbytesPerSec,
				},
				ErrInfo: &pbpo.ErrInfo{
					IsErr:     dsVdBe.VdBeInfo.ErrInfo.IsErr,
					ErrMsg:    dsVdBe.VdBeInfo.ErrInfo.ErrMsg,
					Timestamp: dsVdBe.VdBeInfo.ErrInfo.Timestamp,
				},
			}
			vdBeList = append(vdBeList, poVdBe)
		}
		physicalDisk := &pbpo.PhysicalDisk{
			PdId:        targetPd.PdId,
			PdName:      targetPd.PdName,
			Description: targetPd.PdConf.Description,
			IsOffline:   targetPd.PdConf.IsOffline,
			TotalSize:   targetPd.Capacity.TotalSize,
			FreeSize:    targetPd.Capacity.FreeSize,
			TotalQos: &pbpo.BdevQos{
				RwIosPerSec:    targetPd.Capacity.TotalQos.RwIosPerSec,
				RwMbytesPerSec: targetPd.Capacity.TotalQos.RwMbytesPerSec,
				RMbytesPerSec:  targetPd.Capacity.TotalQos.RMbytesPerSec,
				WMbytesPerSec:  targetPd.Capacity.TotalQos.WMbytesPerSec,
			},
			FreeQos: &pbpo.BdevQos{
				RwIosPerSec:    targetPd.Capacity.FreeQos.RwIosPerSec,
				RwMbytesPerSec: targetPd.Capacity.FreeQos.RwMbytesPerSec,
				RMbytesPerSec:  targetPd.Capacity.FreeQos.RMbytesPerSec,
				WMbytesPerSec:  targetPd.Capacity.FreeQos.WMbytesPerSec,
			},
			ErrInfo: &pbpo.ErrInfo{
				IsErr:     targetPd.PdInfo.ErrInfo.IsErr,
				ErrMsg:    targetPd.PdInfo.ErrInfo.ErrMsg,
				Timestamp: targetPd.PdInfo.ErrInfo.Timestamp,
			},
			VdBeList: vdBeList,
		}
		switch x := targetPd.PdConf.BdevType.(type) {
		case *pbds.PdConf_BdevMalloc:
			physicalDisk.BdevType = &pbpo.PhysicalDisk_BdevMalloc{
				BdevMalloc: &pbpo.BdevMalloc{
					Size: x.BdevMalloc.Size,
				},
			}
		case *pbds.PdConf_BdevAio:
			physicalDisk.BdevType = &pbpo.PhysicalDisk_BdevAio{
				BdevAio: &pbpo.BdevAio{
					FileName: x.BdevAio.FileName,
				},
			}
		case *pbds.PdConf_BdevNvme:
			physicalDisk.BdevType = &pbpo.PhysicalDisk_BdevNvme{
				BdevNvme: &pbpo.BdevNvme{
					TrAddr: x.BdevNvme.TrAddr,
				},
			}
		default:
			logger.Error("Unknow BdevType: %v", diskNode)
			return &pbpo.GetPdReply{
				ReplyInfo: &pbpo.ReplyInfo{
					ReqId:     lib.GetReqId(ctx),
					ReplyCode: lib.PortalInternalErrCode,
					ReplyMsg:  "Unknow BdevType",
				},
			}, nil
		}
		return &pbpo.GetPdReply{
			ReplyInfo: &pbpo.ReplyInfo{
				ReqId:     lib.GetReqId(ctx),
				ReplyCode: lib.PortalSucceedCode,
				ReplyMsg:  lib.PortalSucceedMsg,
			},
			PhysicalDisk: physicalDisk,
		}, nil
	}
}
