package lib

import (
	"context"

	"github.com/coreos/etcd/clientv3"
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
	etcdCli *clientv3.Client
	kf      *KeyFmt
}

func (alloc *Allocator) AllocDnPd(ctx context.Context, vdCnt uint32,
	vdSize uint64, qos *BdevQos) ([]*DnPdCand, error) {
	// opts := []clientv3.OpOption{
	// 	clientv3.WithPrefix(),
	// 	clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend),
	// 	clientv3.WithLimit(3),
	// }
	// kv := clientv3.NewKV(alloc.etcdCli)
	// gr, err := (ctx, prefix, opts...)
	return nil, nil
}

func (alloc *Allocator) AllocCn(ctx context.Context, cnCnt uint32) (
	[]*CnCand, error) {
	return nil, nil
}

func NewAllocator(etcdCli *clientv3.Client, kf *KeyFmt) *Allocator {
	return &Allocator{
		etcdCli: etcdCli,
		kf:      kf,
	}
}
