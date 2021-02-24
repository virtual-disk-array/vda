package portal

import (
	"github.com/coreos/etcd/clientv3"

	"github.com/virtual-disk-array/vda/pkg/lib"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

type portalServer struct {
	pbpo.UnimplementedPortalServer
	etcdCli *clientv3.Client
	kf      *lib.KeyFmt
	sw      *lib.StmWrapper
	sm      *lib.SyncupManager
	alloc   *lib.Allocator
}

func newPortalServer(etcdCli *clientv3.Client) *portalServer {
	kf := lib.NewKeyFmt(lib.DefaultEtcdPrefix)
	sw := lib.NewStmWrapper(etcdCli)
	gc := lib.NewGrpcCache(lib.GrpcCacheTTL, lib.GrpcCacheStep, lib.GrpcCacheInterval)
	sm := lib.NewSyncupManager(kf, sw, gc)
	boundList := make([]uint64, 0)
	boundList = append(boundList, 100*1024*1024*1024)
	boundList = append(boundList, 1000*1024*1024*1024)
	boundList = append(boundList, 0xffffffffffffffff)
	alloc := lib.NewAllocator(etcdCli, kf, boundList)
	return &portalServer{
		etcdCli: etcdCli,
		kf:      kf,
		sw:      sw,
		sm:      sm,
		alloc:   alloc,
	}
}
