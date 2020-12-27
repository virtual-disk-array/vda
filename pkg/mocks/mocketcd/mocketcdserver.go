package mocketcd

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
)

const (
	etcdDir = "/tmp/vdatest.default.etcd"
)

type MockEtcdServer struct {
	e   *embed.Etcd
	cli *clientv3.Client
	t   *testing.T
}

func (s *MockEtcdServer) Stop() {
	s.e.Close()
	s.cli.Close()
	os.RemoveAll(etcdDir)
}

func (s *MockEtcdServer) Put(ctx context.Context, key string, val []byte) error {
	kv := clientv3.NewKV(s.cli)
	_, err := kv.Put(ctx, key, string(val))
	return err
}

func (s *MockEtcdServer) Get(ctx context.Context, key string) ([]byte, error) {
	kv := clientv3.NewKV(s.cli)
	gr, err := kv.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(gr.Kvs) != 1 {
		return nil, nil
	}
	return gr.Kvs[0].Value, nil
}

func (s *MockEtcdServer) Client() *clientv3.Client {
	return s.cli
}

func NewMockEtcdServer(port string, t *testing.T) (*MockEtcdServer, error) {
	os.RemoveAll(etcdDir)
	cfg := embed.NewConfig()
	cfg.Dir = etcdDir
	listenClientURL, err := url.Parse("http://127.0.0.1:" + port)
	if err != nil {
		t.Errorf("parse listenClientURL err: %v", err)
		return nil, err
	}
	cfg.LCUrls = []url.URL{*listenClientURL}
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Errorf("StartEtcd err: %v", err)
		return nil, err
	}

	endpoints := []string{"127.0.0.1:" + port}
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		t.Errorf("create etcd client err: %v", err)
		e.Close()
		return nil, err
	}

	return &MockEtcdServer{
		e:   e,
		cli: cli,
		t:   t,
	}, nil
}
