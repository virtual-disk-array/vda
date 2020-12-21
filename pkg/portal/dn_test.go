package portal

import (
	// "context"
	"os"
	"testing"

	"github.com/coreos/etcd/embed"
)

func TestAbc(t *testing.T) {
	etcdDir := "/tmp/test.default.etcd"
	os.Remove(etcdDir)
	cfg := embed.NewConfig()
	cfg.Dir = "/tmp/test.default.etcd"
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Errorf("StartEtcd err: %v", err)
	}
	defer e.Close()
}
