package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/protobuf/proto"
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

type listArgsStruct struct {
	prefix string
	from   string
	limit  int64
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

	listCmd = &cobra.Command{
		Use:  "list",
		Args: cobra.MaximumNArgs(0),
		Run:  keyListFunc,
	}
	listArgs = &listArgsStruct{}
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

	listCmd.Flags().StringVarP(&listArgs.prefix, "prefix", "", "",
		"list keys with the same prefix")
	listCmd.Flags().StringVarP(&listArgs.from, "from", "", "",
		"list keys from this key")
	listCmd.Flags().Int64VarP(&listArgs.limit, "limit", "", 100,
		"max items to return")
	adminCmd.AddCommand(listCmd)
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
				} else if strings.HasPrefix(key, adm.kf.DnListPrefix()) {
					m = &pbds.DnSummary{}
				} else if strings.HasPrefix(key, adm.kf.DnErrPrefix()) {
					m = &pbds.DnSummary{}
				} else if strings.HasPrefix(key, adm.kf.CnEntityPrefix()) {
					m = &pbds.ControllerNode{}
				} else if strings.HasPrefix(key, adm.kf.CnListPrefix()) {
					m = &pbds.CnSummary{}
				} else if strings.HasPrefix(key, adm.kf.CnErrPrefix()) {
					m = &pbds.CnSummary{}
				} else if strings.HasPrefix(key, adm.kf.DaEntityPrefix()) {
					m = &pbds.DiskArray{}
				} else if strings.HasPrefix(key, adm.kf.DaListPrefix()) {
					m = &pbds.DaSummary{}
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

type mapObject struct {
	m map[string]interface{}
}

func (mapObj *mapObject) hasKey(key string) bool {
	_, ok := mapObj.m[key]
	return ok
}

func (mapObj *mapObject) getStr(key string) string {
	valRaw, ok := mapObj.m[key]
	if !ok {
		return ""
	}
	val, ok := valRaw.(string)
	if !ok {
		logger.Fatal("Can not convert %s to string: %v", key, valRaw)
	}
	return val
}

func (mapObj *mapObject) getUint32(key string) uint32 {
	valRaw, ok := mapObj.m[key]
	if !ok {
		return uint32(0)
	}
	valNum, ok := valRaw.(json.Number)
	if !ok {
		logger.Fatal("Can not convert %s to json.Number: %v", key, valRaw)
	}
	val, err := valNum.Int64()
	if err != nil {
		logger.Fatal("Can not convert %s to int32: %v", key, valNum)
	}
	return uint32(val)
}

func (mapObj *mapObject) getUint64(key string) uint64 {
	valRaw, ok := mapObj.m[key]
	if !ok {
		return uint64(0)
	}
	valNum, ok := valRaw.(json.Number)
	if !ok {
		logger.Fatal("Can not convert %s to json.Number: %v", key, valRaw)
	}
	val, err := valNum.Int64()
	if err != nil {
		logger.Fatal("Can not convert %s to int64: %v", key, valNum)
	}
	return uint64(val)
}

func (mapObj *mapObject) getBool(key string) bool {
	valRaw, ok := mapObj.m[key]
	if !ok {
		return false
	}
	val, ok := valRaw.(bool)
	if !ok {
		logger.Fatal("Can not convert %s to bool: %v", key, valRaw)
	}
	return val
}

func (mapObj *mapObject) getMapObj(key string) *mapObject {
	valRaw, ok := mapObj.m[key]
	if !ok {
		return &mapObject{make(map[string]interface{})}
	}
	val, ok := valRaw.(map[string]interface{})
	if !ok {
		logger.Fatal("Can not convert %s to map[string]interface{}: %v", key, valRaw)
	}
	return &mapObject{val}
}

func (mapObj *mapObject) getMapObjList(key string) []*mapObject {
	valRaw, ok := mapObj.m[key]
	if !ok {
		return make([]*mapObject, 0)
	}
	items, ok := valRaw.([]interface{})
	if !ok {
		logger.Fatal("Can not convert %s to []interface{}: %v", key, valRaw)
	}
	mapObjList := make([]*mapObject, 0)
	for _, itemRaw := range items {
		item, ok := itemRaw.(map[string]interface{})
		if !ok {
			logger.Fatal("Can not convert %v to map[string]interface{}", item)
		}
		mapObj := &mapObject{item}
		mapObjList = append(mapObjList, mapObj)
	}
	return mapObjList
}

func (adm *admin) getDnEntity(value interface{}) proto.Message {
	dnMap, ok := value.(map[string]interface{})
	if !ok {
		logger.Fatal("Can not convert to dnMap map[string]interface{}")
	}

	dnObj := mapObject{dnMap}
	dnConfObj := dnObj.getMapObj("dn_conf")
	nvmfLisObj := dnConfObj.getMapObj("nvmf_listener")
	dnInfoObj := dnObj.getMapObj("dn_info")
	dnErrObj := dnInfoObj.getMapObj("err_info")

	pdList := make([]*pbds.PhysicalDisk, 0)
	for _, pdObj := range dnObj.getMapObjList("pd_list") {
		pdConfObj := pdObj.getMapObj("pd_conf")
		pdInfoObj := pdObj.getMapObj("pd_info")
		pdErrInfoObj := pdInfoObj.getMapObj("err_info")
		capObj := pdObj.getMapObj("capacity")
		totalQosObj := capObj.getMapObj("total_qos")
		freeQosObj := capObj.getMapObj("free_qos")
		vdBeList := make([]*pbds.VdBackend, 0)
		for _, vdBeObj := range pdObj.getMapObjList("vd_be_list") {
			vdBeConfObj := vdBeObj.getMapObj("vd_be_conf")
			qosObj := vdBeConfObj.getMapObj("qos")
			vdBeInfoObj := vdBeObj.getMapObj("vd_be_info")
			vdBeErrInfoObj := vdBeInfoObj.getMapObj("err_info")
			vdBe := &pbds.VdBackend{
				VdId: vdBeObj.getStr("vd_id"),
				VdBeConf: &pbds.VdBeConf{
					DaName: vdBeConfObj.getStr("da_name"),
					GrpIdx: vdBeConfObj.getUint32("grp_idx"),
					VdIdx:  vdBeConfObj.getUint32("vd_idx"),
					Size:   vdBeConfObj.getUint64("size"),
					Qos: &pbds.BdevQos{
						RwIosPerSec:    qosObj.getUint64("rw_ios_per_sec"),
						RwMbytesPerSec: qosObj.getUint64("rw_mbytes_per_sec"),
						RMbytesPerSec:  qosObj.getUint64("r_mbytes_per_sec"),
						WMbytesPerSec:  qosObj.getUint64("w_mbytes_per_sec"),
					},
				},
				VdBeInfo: &pbds.VdBeInfo{
					ErrInfo: &pbds.ErrInfo{
						IsErr:     vdBeErrInfoObj.getBool("is_err"),
						ErrMsg:    vdBeErrInfoObj.getStr("err_msg"),
						Timestamp: vdBeErrInfoObj.getStr("timestamp"),
					},
				},
			}
			vdBeList = append(vdBeList, vdBe)
		}
		pd := &pbds.PhysicalDisk{
			PdId:   pdObj.getStr("pd_id"),
			PdName: pdObj.getStr("pd_name"),
			PdConf: &pbds.PdConf{
				Description: pdConfObj.getStr("description"),
				IsOffline:   pdConfObj.getBool("is_offline"),
			},
			PdInfo: &pbds.PdInfo{
				ErrInfo: &pbds.ErrInfo{
					IsErr:     pdErrInfoObj.getBool("is_err"),
					ErrMsg:    pdErrInfoObj.getStr("err_msg"),
					Timestamp: pdErrInfoObj.getStr("timestamp"),
				},
			},
			Capacity: &pbds.PdCapacity{
				TotalSize: capObj.getUint64("total_size"),
				FreeSize:  capObj.getUint64("free_size"),
				TotalQos: &pbds.BdevQos{
					RwIosPerSec:    totalQosObj.getUint64("rw_ios_per_sec"),
					RwMbytesPerSec: totalQosObj.getUint64("rw_mbytes_per_sec"),
					RMbytesPerSec:  totalQosObj.getUint64("r_mbytes_per_sec"),
					WMbytesPerSec:  totalQosObj.getUint64("w_mbytes_per_sec"),
				},
				FreeQos: &pbds.BdevQos{
					RwIosPerSec:    freeQosObj.getUint64("rw_ios_per_sec"),
					RwMbytesPerSec: freeQosObj.getUint64("rw_mbytes_per_sec"),
					RMbytesPerSec:  freeQosObj.getUint64("r_mbytes_per_sec"),
					WMbytesPerSec:  freeQosObj.getUint64("w_mbytes_per_sec"),
				},
			},
			VdBeList: vdBeList,
		}
		bdevTypeObj := pdConfObj.getMapObj("BdevType")
		if bdevTypeObj.hasKey("BdevMalloc") {
			bdevMallocObj := bdevTypeObj.getMapObj("BdevMalloc")
			pd.PdConf.BdevType = &pbds.PdConf_BdevMalloc{
				BdevMalloc: &pbds.BdevMalloc{
					Size: bdevMallocObj.getUint64("size"),
				},
			}
		} else if bdevTypeObj.hasKey("BdevAio") {
			bdevAioObj := bdevTypeObj.getMapObj("BdevAio")
			pd.PdConf.BdevType = &pbds.PdConf_BdevAio{
				BdevAio: &pbds.BdevAio{
					FileName: bdevAioObj.getStr("file_name"),
				},
			}
		} else if bdevTypeObj.hasKey("BdevNvme") {
			bdevNvmeObj := bdevTypeObj.getMapObj("BdevNvme")
			pd.PdConf.BdevType = &pbds.PdConf_BdevNvme{
				BdevNvme: &pbds.BdevNvme{
					TrAddr: bdevNvmeObj.getStr("tr_addr"),
				},
			}
		} else {
			logger.Fatal("Unknow BdevType: %v", bdevTypeObj)
		}
		pdList = append(pdList, pd)
	}

	diskNode := &pbds.DiskNode{
		DnId:     dnObj.getStr("dn_id"),
		SockAddr: dnObj.getStr("sock_addr"),
		DnConf: &pbds.DnConf{
			Description: dnConfObj.getStr("description"),
			NvmfListener: &pbds.NvmfListener{
				TrType:  nvmfLisObj.getStr("tr_type"),
				AdrFam:  nvmfLisObj.getStr("adr_fam"),
				TrAddr:  nvmfLisObj.getStr("tr_addr"),
				TrSvcId: nvmfLisObj.getStr("tr_svc_id"),
			},
			Location:  dnConfObj.getStr("location"),
			IsOffline: dnConfObj.getBool("is_offline"),
			HashCode:  dnConfObj.getUint32("hash_code"),
		},
		DnInfo: &pbds.DnInfo{
			ErrInfo: &pbds.ErrInfo{
				IsErr:     dnErrObj.getBool("is_err"),
				ErrMsg:    dnErrObj.getStr("err_msg"),
				Timestamp: dnErrObj.getStr("timestamp"),
			},
		},
		PdList: pdList,
	}
	return diskNode
}

func (adm *admin) getDnSummary(value interface{}) proto.Message {
	return nil
}

func (adm *admin) getCnEntity(value interface{}) proto.Message {
	return nil
}

func (adm *admin) getCnSummary(value interface{}) proto.Message {
	return nil
}

func (adm *admin) getDaEntity(value interface{}) proto.Message {
	return nil
}

func (adm *admin) getDaSummary(value interface{}) proto.Message {
	return nil
}

func (adm *admin) load(args *loadArgsStruct) {
	data, err := ioutil.ReadFile(args.filePath)
	if err != nil {
		logger.Fatal("ReadFile err: %v", err)
	}
	keyPairList := make([]*KeyPair, 0)
	d := json.NewDecoder(strings.NewReader(string(data)))
	d.UseNumber()
	if err := d.Decode(&keyPairList); err != nil {
		logger.Fatal("Decode err: %v", err)
	}
	apply := func(stm concurrency.STM) error {
		for _, keyPair := range keyPairList {
			revision := stm.Rev(keyPair.Key)
			if revision != keyPair.Revision {
				logger.Fatal("Key has new revision: %s %v", keyPair.Key, revision)
			}
			if keyPair.Value == nil {
				stm.Del(keyPair.Key)
			} else {
				var m proto.Message
				var err error
				if strings.HasPrefix(keyPair.Key, adm.kf.DnEntityPrefix()) {
					m = adm.getDnEntity(keyPair.Value)
				} else if strings.HasPrefix(keyPair.Key, adm.kf.DnListPrefix()) {
					m = adm.getDnSummary(keyPair.Value)
				} else if strings.HasPrefix(keyPair.Key, adm.kf.DnErrPrefix()) {
					m = adm.getDnSummary(keyPair.Value)
				} else if strings.HasPrefix(keyPair.Key, adm.kf.CnEntityPrefix()) {
					m = adm.getCnEntity(keyPair.Value)
				} else if strings.HasPrefix(keyPair.Key, adm.kf.CnListPrefix()) {
					m = adm.getCnSummary(keyPair.Value)
				} else if strings.HasPrefix(keyPair.Key, adm.kf.CnErrPrefix()) {
					m = adm.getCnSummary(keyPair.Value)
				} else if strings.HasPrefix(keyPair.Key, adm.kf.DaEntityPrefix()) {
					m = adm.getDaEntity(keyPair.Value)
				} else if strings.HasPrefix(keyPair.Key, adm.kf.DaListPrefix()) {
					m = adm.getDaSummary(keyPair.Value)
				} else {
					logger.Fatal("Unknow key: %s", keyPair.Key)
				}
				value, err := proto.Marshal(m)
				if err != nil {
					logger.Fatal("Marshal err: %s %v", keyPair.Key, err)
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

func (adm *admin) list(args *listArgsStruct) {
	if args.prefix != "" && args.from != "" {
		logger.Fatal("Can not set prefix and from at the same time")
	}
	if args.prefix == "" && args.from == "" {
		logger.Fatal("Should provide either prefix or from")
	}
	opts := []clientv3.OpOption{
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(args.limit),
	}
	var key string
	if args.prefix != "" {
		opts = append(opts, clientv3.WithPrefix())
		key = args.prefix
	} else {
		opts = append(opts, clientv3.WithFromKey())
		key = args.from
	}
	kv := clientv3.NewKV(adm.etcdCli)
	gr, err := kv.Get(adm.ctx, key, opts...)
	if err != nil {
		logger.Fatal("kv.Get err: %v", err)
	}
	for _, item := range gr.Kvs {
		fmt.Println(string(item.Key))
	}
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

func keyListFunc(cmd *cobra.Command, args []string) {
	adm := newAdmin(adminArgs)
	defer adm.close()
	adm.list(listArgs)
}

func Execute() {
	if err := adminCmd.Execute(); err != nil {
		logger.Fatal("Execute err: %v", err)
	}
}
