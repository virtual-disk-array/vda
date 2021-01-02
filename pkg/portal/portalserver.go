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
	sm := lib.NewSyncupManager(kf, sw)
	alloc := lib.NewAllocator(etcdCli, kf)
	return &portalServer{
		etcdCli: etcdCli,
		kf:      kf,
		sw:      sw,
		sm:      sm,
		alloc:   alloc,
	}
}
