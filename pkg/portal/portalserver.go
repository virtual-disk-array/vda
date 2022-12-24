package portal

import (
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/virtual-disk-array/vda/pkg/lib"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

type portalServer struct {
	pbpo.UnimplementedPortalServer
	etcdCli *clientv3.Client
	kf      *lib.KeyFmt
	sw      *lib.StmWrapper
	sm      *lib.SyncupManager
	nf      *lib.NameFmt
	alloc   *lib.Allocator
}

func newPortalServer(etcdCli *clientv3.Client) *portalServer {
	kf := lib.NewKeyFmt(lib.DefaultEtcdPrefix)
	sw := lib.NewStmWrapper(etcdCli)
	sm := lib.NewSyncupManager(kf, sw)
	nf := lib.NewNameFmt(lib.DefaultVdaPrefix, lib.DefaultNqnPrefix)
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
		nf:      nf,
		alloc:   alloc,
	}
}
