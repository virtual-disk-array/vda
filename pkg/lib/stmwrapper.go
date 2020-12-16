package lib

import (
	"context"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"

	"github.com/virtual-disk-array/vda/pkg/logger"
)

type StmWrapper struct {
	etcdCli *clientv3.Client
}

func (sw *StmWrapper) RunStm(apply func(stm concurrency.STM) error,
	ctx context.Context, name string) error {
	cnt := 0
	applyWrapper := func(stm concurrency.STM) error {
		cnt++
		logger.Info("stm apply, %s %d", name, cnt)
		return apply(stm)
	}
	_, err := concurrency.NewSTM(sw.etcdCli, applyWrapper, concurrency.WithAbortContext(ctx))
	return err
}

func NewStmWrapper(etcdCli *clientv3.Client) *StmWrapper {
	return &StmWrapper{
		etcdCli: etcdCli,
	}
}
