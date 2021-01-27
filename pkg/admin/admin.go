package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbds "github.com/virtual-disk-array/vda/pkg/proto/dataschema"
)

type adminArgsStruct struct {
	etcdEndpoints string
	timeout       int
}

type dumpArgsStruct struct {
	keys     string
	filePath string
}

type loadArgsStruct struct {
	filePath string
}

var (
	adminCmd = &cobra.Command{
		Use:   "vda_admin",
		Short: "vda admin",
		Long:  `vda admin`,
	}
	adminArgs = &adminArgsStruct{}

	dumpCmd = &cobra.Command{
		Use:  "dump",
		Args: cobra.MaximumNArgs(0),
		Run:  keyDumpFunc,
	}
	dumpArgs = &dumpArgsStruct{}

	loadCmd = &cobra.Command{
		Use:  "load",
		Args: cobra.MaximumNArgs(0),
		Run:  keyLoadFunc,
	}
	loadArgs = &loadArgsStruct{}
)

func init() {
	adminCmd.PersistentFlags().StringVarP(
		&adminArgs.etcdEndpoints, "etcd-endpoints", "", "localhost:2379",
		"etcd endpoint list")
	adminCmd.PersistentFlags().IntVarP(
		&adminArgs.timeout, "timeout", "", 30,
		"etcd access timeout")

	dumpCmd.Flags().StringVarP(&dumpArgs.keys, "keys", "", "",
		"keys to dump, splited by comma")
	dumpCmd.MarkFlagRequired("keys")
	dumpCmd.Flags().StringVarP(&dumpArgs.filePath, "file-path", "", "",
		"output file path")
	dumpCmd.MarkFlagRequired("file-path")
	adminCmd.AddCommand(dumpCmd)

	loadCmd.Flags().StringVarP(&loadArgs.filePath, "file-path", "", "",
		"json file that contains the keyPairs")
	loadCmd.MarkFlagRequired("file-path")
	adminCmd.AddCommand(loadCmd)
}

type KeyPair struct {
	Key      string
	Revision int64
	Value    interface{}
}

type admin struct {
	etcdCli *clientv3.Client
	kf      *lib.KeyFmt
	sw      *lib.StmWrapper
	ctx     context.Context
	cancel  context.CancelFunc
}

func (adm *admin) close() {
	adm.cancel()
	adm.etcdCli.Close()
}

func (adm *admin) dump(args *dumpArgsStruct) {
	keyList := strings.Split(args.keys, ",")
	keyPairList := make([]*KeyPair, 0)

	apply := func(stm concurrency.STM) error {
		for _, key := range keyList {
			var keyPair *KeyPair
			revision := stm.Rev(key)
			if revision == 0 {
				keyPair = &KeyPair{
					Key:      key,
					Revision: revision,
					Value:    nil,
				}
			} else {
				pbVal := []byte(stm.Get(key))
				var m proto.Message
				if strings.HasPrefix(key, adm.kf.DnEntityPrefix()) {
					m = &pbds.DiskNode{}
				} else {
					return fmt.Errorf("Unknow key: %s", key)
				}
				if err := proto.Unmarshal(pbVal, m); err != nil {
					return fmt.Errorf("Can not Unmarshal %s %v", key, err)
				}
				keyPair = &KeyPair{
					Key:      key,
					Revision: revision,
					Value:    m,
				}
			}
			keyPairList = append(keyPairList, keyPair)
		}
		return nil
	}

	if err := adm.sw.RunStm(apply, adm.ctx, "DumpKey"); err != nil {
		logger.Fatal("DumpKey err: %v", err)
	}
	output, err := json.MarshalIndent(keyPairList, "", "  ")
	if err != nil {
		logger.Fatal("Marshal err: %v %v", keyPairList, err)
	}
	if err := ioutil.WriteFile(args.filePath, output, 0644); err != nil {
		logger.Fatal("WriteFile err: %v", err)
	}
	logger.Info("Done")
}

func (adm *admin) getDnEntity(value interface{}) (proto.Message, error) {
	diskNode := &pbds.DiskNode{}
	dnMap, ok := value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Can not convert to dnMap map[string]interface{}")
	}
	dnIdRaw, ok := dnMap["dn_id"]
	if !ok {
		return nil, fmt.Errorf("Can not find dn_id")
	}
	dnId, ok := dnIdRaw.(string)
	if !ok {
		return nil, fmt.Errorf("Can not convert dnId string")
	}
	diskNode.DnId = dnId
	return diskNode, nil
}

func (adm *admin) load(args *loadArgsStruct) {
	data, err := ioutil.ReadFile(args.filePath)
	if err != nil {
		logger.Fatal("ReadFile err: %v", err)
	}
	keyPairList := make([]*KeyPair, 0)
	if err := json.Unmarshal(data, &keyPairList); err != nil {
		logger.Fatal("Unmarshal keyPairList err: %v", err)
	}
	apply := func(stm concurrency.STM) error {
		for _, keyPair := range keyPairList {
			revision := stm.Rev(keyPair.Key)
			if revision != keyPair.Revision {
				return fmt.Errorf("Key has new revision: %s %v", keyPair.Key, revision)
			}
			if keyPair.Value == nil {
				stm.Del(keyPair.Key)
			} else {
				var m proto.Message
				var err error
				if strings.HasPrefix(keyPair.Key, adm.kf.DnEntityPrefix()) {
					m, err = adm.getDnEntity(keyPair.Value)
				} else {
					return fmt.Errorf("Unknow key: %s", keyPair.Key)
				}
				if err != nil {
					return fmt.Errorf("Decode err: %s %v", keyPair.Key, err)
				}
				value, err := proto.Marshal(m)
				if err != nil {
					return fmt.Errorf("Marshal err: %s %v", keyPair.Key, err)
				}
				stm.Put(keyPair.Key, string(value))
			}
		}
		return nil
	}

	if err := adm.sw.RunStm(apply, adm.ctx, "LoadKey"); err != nil {
		logger.Fatal("LoadKey err: %v", err)
	}
	logger.Info("Done")
}

func newAdmin(args *adminArgsStruct) *admin {
	etcdEndpoints := strings.Split(args.etcdEndpoints, ",")
	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: etcdEndpoints})
	if err != nil {
		logger.Fatal("Create etcd client err: %v", err)
	}
	kf := lib.NewKeyFmt(lib.DefaultEtcdPrefix)
	sw := lib.NewStmWrapper(etcdCli)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(args.timeout)*time.Second)
	return &admin{
		etcdCli: etcdCli,
		kf:      kf,
		sw:      sw,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func keyDumpFunc(cmd *cobra.Command, args []string) {
	adm := newAdmin(adminArgs)
	defer adm.close()
	adm.dump(dumpArgs)
}

func keyLoadFunc(cmd *cobra.Command, args []string) {
	adm := newAdmin(adminArgs)
	defer adm.close()
	adm.load(loadArgs)
}

func Execute() {
	if err := adminCmd.Execute(); err != nil {
		logger.Fatal("Execute err: %v", err)
	}
}
