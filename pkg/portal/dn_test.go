package portal

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
)

const (
	etcdPort = "30000"
)

func TestAbc(t *testing.T) {
	etcdDir := "/tmp/test.default.etcd"
	err := os.RemoveAll(etcdDir)
	if err != nil {
		t.Errorf("remove dir err: %v", err)
	}
	cfg := embed.NewConfig()
	cfg.Dir = "/tmp/test.default.etcd"
	listenClientURL, err := url.Parse("http://127.0.0.1:" + etcdPort)
	if err != nil {
		t.Errorf("parse listenClientURL err: %v", err)
		return
	}
	cfg.LCUrls = []url.URL{*listenClientURL}
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Errorf("StartEtcd err: %v", err)
		return
	}
	defer e.Close()

	endpoints := []string{"127.0.0.1:" + etcdPort}
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		t.Errorf("create etcd client err: %v", err)
		return
	}
	defer cli.Close()

	ctx := context.Background()
	kv := clientv3.NewKV(cli)

	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(3),
	}
	gr, err := kv.Get(ctx, "test", opts...)
	if err != nil {
		t.Errorf("get key err: %v", err)
		return
	}
	cnt := len(gr.Kvs)
	if cnt != 0 {
		t.Errorf("get key result is not 0: %v %s", cnt, gr.Kvs[0].Value)
		return
	}

	_, err = kv.Put(ctx, "test1", "foo")
	if err != nil {
		t.Errorf("put key err: %v", err)
		return
	}
}
