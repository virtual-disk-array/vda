package portal

import (
	"github.com/coreos/etcd/clientv3"
	"google.golang.org/grpc"

	"github.com/virtual-disk-array/vda/pkg/lib"
	pbpo "github.com/virtual-disk-array/vda/pkg/proto/portalapi"
)

type portalServer struct {
	pbpo.UnimplementedPortalServer
	etcdCli    *clientv3.Client
	kf         *lib.KeyFmt
	sw         *lib.StmWrapper
	sm         *lib.SyncupManager
	createConn func(sockAddr string) (*grpc.ClientConn, error)
}

func newPortalServer(etcdCli *clientv3.Client,
	createConn func(sockAddr string) (*grpc.ClientConn, error)) *portalServer {
	kf := lib.NewKeyFmt(lib.DefaultEtcdPrefix)
	sw := lib.NewStmWrapper(etcdCli)
	sm := lib.NewSyncupManager(kf, sw, createConn)
	return &portalServer{
		etcdCli:    etcdCli,
		kf:         kf,
		sw:         sw,
		sm:         sm,
		createConn: createConn,
	}
}
