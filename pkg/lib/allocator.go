package lib

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/virtual-disk-array/vda/pkg/logger"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
)

type DnPdCand struct {
	SockAddr string
	PdName   string
}

type CnCand struct {
	SockAddr string
}

type BdevQos struct {
	RwIosPerSec    uint64
	RwMbytesPerSec uint64
	RMbytesPerSec  uint64
	WMbytesPerSec  uint64
}

type Allocator struct {
	etcdCli   *clientv3.Client
	kf        *KeyFmt
	pageSize  int64
	boundList []uint64
}

type dnPdContext struct {
	ctx      context.Context
	vdSize   uint64
	qos      *BdevQos
	cnt      uint32
	locMap   map[string]bool
	candList []*DnPdCand
}

type cnContext struct {
	ctx      context.Context
	cnt      uint32
	locMap   map[string]bool
	candList []*CnCand
}

func (alloc *Allocator) allocDnPdByBoundary(dnPdCtx *dnPdContext,
	highBound, lowBound uint64) error {
	opts := []clientv3.OpOption{
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(alloc.pageSize),
		clientv3.WithFromKey(),
	}
	lastKey := alloc.kf.DnCapSizePrefix(highBound)
	startIdx := 0
	kv := clientv3.NewKV(alloc.etcdCli)
	logger.Info("allocDnPdByBoundary: %v %d %d",
		dnPdCtx, highBound, lowBound)
	for {
		gr, err := kv.Get(dnPdCtx.ctx, lastKey, opts...)
		if err != nil {
			logger.Error("kv.Get err: %s %v", lastKey, err)
			return err
		}
		logger.Info("kv.Get len: %s %d", lastKey, len(gr.Kvs))
		if len(gr.Kvs)-startIdx == 0 {
			break
		}
		for _, item := range gr.Kvs[startIdx:] {
			key := string(item.Key)
			lastKey = key
			startIdx = 1
			val := item.Value
			freeSize, sockAddr, pdName, err := alloc.kf.DecodeDnCapKey(key)
			if err != nil {
				if serr, ok := err.(*InvalidKeyError); ok {
					logger.Info("DnPdCap at the end: %v", serr)
					return nil
				} else {
					logger.Warning("DecodeDnCapKey err: %v", err)
					continue
				}
			}
			if freeSize < lowBound {
				logger.Info("Out of low boundary: %s %d %d",
					key, freeSize, lowBound)
				return nil
			}
			if freeSize < dnPdCtx.vdSize {
				logger.Info("Less than vdSize: %s %d %d",
					key, freeSize, dnPdCtx.vdSize)
				return nil
			}
			attr := &pbds.DnSearchAttr{}
			if err := proto.Unmarshal(val, attr); err != nil {
				logger.Warning("Unmarshal DnSearchAttr err: %s %v", key, err)
				continue
			}
			if attr.PdCapacity.FreeQos.RwIosPerSec < dnPdCtx.qos.RwIosPerSec {
				continue
			}
			if attr.PdCapacity.FreeQos.RwMbytesPerSec < dnPdCtx.qos.RwMbytesPerSec {
				continue
			}
			if attr.PdCapacity.FreeQos.RMbytesPerSec < dnPdCtx.qos.RMbytesPerSec {
				continue
			}
			if attr.PdCapacity.FreeQos.WMbytesPerSec < dnPdCtx.qos.WMbytesPerSec {
				continue
			}
			if attr.Location != "" {
				_, ok := dnPdCtx.locMap[attr.Location]
				if ok {
					continue
				}
			}
			diskNode := &pbds.DiskNode{}
			dnEntityKey := alloc.kf.DnEntityKey(sockAddr)
			gr1, err := kv.Get(dnPdCtx.ctx, dnEntityKey)
			if err != nil {
				logger.Error("Get dnEntityKey err: %s %v", dnEntityKey, err)
				return err
			}
			if len(gr1.Kvs) < 1 {
				logger.Warning("Can not find dnEntityKey: %s", dnEntityKey)
				continue
			}
			if err := proto.Unmarshal(gr1.Kvs[0].Value, diskNode); err != nil {
				logger.Warning("Unmarshal diskNode err: %s %v", dnEntityKey, err)
				continue
			}
			if diskNode.DnConf.IsOffline {
				continue
			}
			if diskNode.DnInfo.ErrInfo.IsErr {
				continue
			}
			var targetPd *pbds.PhysicalDisk
			for _, pd := range diskNode.PdList {
				if pdName == pd.PdName {
					targetPd = pd
					break
				}
			}
			if targetPd == nil {
				logger.Warning("Can not find pd: %s %v", pdName, diskNode)
				continue
			}
			if targetPd.PdConf.IsOffline {
				continue
			}
			if targetPd.PdInfo.ErrInfo.IsErr {
				continue
			}
			cand := &DnPdCand{
				SockAddr: sockAddr,
				PdName:   pdName,
			}
			dnPdCtx.candList = append(dnPdCtx.candList, cand)
			if attr.Location != "" {
				dnPdCtx.locMap[attr.Location] = true
			}
			if uint32(len(dnPdCtx.candList)) >= dnPdCtx.cnt {
				return nil
			}
		}
	}
	return nil
}

func (alloc *Allocator) AllocDnPd(ctx context.Context, vdCnt uint32,
	vdSize uint64, qos *BdevQos, dnLocList []string) ([]*DnPdCand, error) {
	dnPdCtx := &dnPdContext{
		ctx:      ctx,
		vdSize:   vdSize,
		qos:      qos,
		cnt:      vdCnt,
		locMap:   make(map[string]bool),
		candList: make([]*DnPdCand, 0),
	}
	for _, loc := range dnLocList {
		dnPdCtx.locMap[loc] = true
	}
	lowBound := uint64(0)
	for _, highBound := range alloc.boundList {
		if highBound > vdSize {
			err := alloc.allocDnPdByBoundary(dnPdCtx, highBound, lowBound)
			if err != nil {
				return nil, err
			}
			if uint32(len(dnPdCtx.candList)) >= vdCnt {
				return dnPdCtx.candList, nil
			}
		}
	}
	logger.Warning("No enough DnPd: %v", dnPdCtx)
	return nil, fmt.Errorf("No enough DnPd")
}

func (alloc *Allocator) allocCn(cnCtx *cnContext) error {
	opts := []clientv3.OpOption{
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(alloc.pageSize),
		clientv3.WithFromKey(),
	}
	lastKey := alloc.kf.CnCapPrefix()
	startIdx := 0
	kv := clientv3.NewKV(alloc.etcdCli)
	logger.Info("allocCn: %v", cnCtx)
	for {
		gr, err := kv.Get(cnCtx.ctx, lastKey, opts...)
		if err != nil {
			logger.Error("kv.Get err: %s %v", lastKey, err)
			return err
		}
		logger.Info("kv.Get len: %s %d", lastKey, len(gr.Kvs))
		if len(gr.Kvs)-startIdx == 0 {
			break
		}
		for _, item := range gr.Kvs[startIdx:] {
			key := string(item.Key)
			lastKey = key
			startIdx = 1
			val := item.Value
			_, sockAddr, err := alloc.kf.DecodeCnCapKey(key)
			if err != nil {
				if serr, ok := err.(*InvalidKeyError); ok {
					logger.Info("CnCap at the end: %v", serr)
					return nil
				} else {
					logger.Warning("DecodeCnCapKey err: %v", err)
					continue
				}
			}
			attr := &pbds.CnSearchAttr{}
			if err := proto.Unmarshal(val, attr); err != nil {
				logger.Warning("Unmarshal CnSearchAttr err: %s %v", key, err)
				continue
			}
			if attr.Location != "" {
				_, ok := cnCtx.locMap[attr.Location]
				if ok {
					continue
				}
			}
			controllerNode := &pbds.ControllerNode{}
			cnEntityKey := alloc.kf.CnEntityKey(sockAddr)
			gr1, err := kv.Get(cnCtx.ctx, cnEntityKey)
			if err != nil {
				logger.Error("Get cnEntityKey err: %s %v", cnEntityKey, err)
				return err
			}
			if len(gr.Kvs) < 1 {
				logger.Warning("Can nto find cnEntityKey: %s", cnEntityKey)
				continue
			}
			if err := proto.Unmarshal(gr1.Kvs[0].Value, controllerNode); err != nil {
				logger.Warning("Unmarshal controllerNode err: %s %v", cnEntityKey, err)
				continue
			}
			if controllerNode.CnConf.IsOffline {
				continue
			}
			if controllerNode.CnInfo.ErrInfo.IsErr {
				continue
			}
			cand := &CnCand{
				SockAddr: sockAddr,
			}
			cnCtx.candList = append(cnCtx.candList, cand)
			if attr.Location != "" {
				cnCtx.locMap[attr.Location] = true
			}
			if uint32(len(cnCtx.candList)) >= cnCtx.cnt {
				return nil
			}
		}
	}
	return nil
}

func (alloc *Allocator) AllocCn(ctx context.Context, cnCnt uint32) (
	[]*CnCand, error) {
	cnCtx := &cnContext{
		ctx:      ctx,
		cnt:      cnCnt,
		locMap:   make(map[string]bool),
		candList: make([]*CnCand, 0),
	}
	err := alloc.allocCn(cnCtx)
	if err != nil {
		return nil, err
	}
	if uint32(len(cnCtx.candList)) >= cnCnt {
		return cnCtx.candList, nil
	}
	logger.Warning("No enough Cn: %v", cnCtx)
	return nil, fmt.Errorf("No enough Cn")
}

func NewAllocator(etcdCli *clientv3.Client, kf *KeyFmt, boundList []uint64) *Allocator {
	return &Allocator{
		etcdCli:   etcdCli,
		kf:        kf,
		boundList: boundList,
	}
}
