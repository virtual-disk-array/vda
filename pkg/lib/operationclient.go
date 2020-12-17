package lib

import (
	"fmt"
	"strings"

	"github.com/virtual-disk-array/vda/pkg/logger"
)

const (
	SPDK_PAGE_SIZE     = uint64(4096)
	CLUSTER_SIZE_MB    = uint64(4)
	CLUSTER_SIZE       = 1024 * 1024 * CLUSTER_SIZE_MB
	NVMF_MODULE_NUMBER = "VDA_CONTROLLER"
)

type LisConf struct {
	TrType  string `json:"trtype"`
	TrAddr  string `json:"traddr"`
	AdrFam  string `json:"adrfam"`
	TrSvcId string `json:"trsvcid"`
}

type BdevSeq struct {
	Name string
	Idx  uint32
}

type Iostat struct {
	TickRate                uint64
	BytesRead               uint64
	NumReadOps              uint64
	BytesWritten            uint64
	NumWriteOps             uint64
	ByetsUnampped           uint64
	NumUnmapOps             uint64
	ReadLatencyTicks        uint64
	WriteLatencyTicks       uint64
	UnmapLatencyTicks       uint64
	QueueDepthPollingPeriod uint64
	QueueDepth              uint64
	IoTime                  uint64
	WeightedIoTime          uint64
}

type LvsInfo struct {
	FreeClusters      uint64
	ClusterSize       uint64
	TotalDataClusters uint64
	BlockSize         uint64
}

type bdevConf struct {
	name           string      `json:"name"`
	aliases        []string    `json:"aliases"`
	productName    string      `json:"product_name"`
	driverSpecific interface{} `json:"driver_specific"`
}

type nvmfConf struct {
	Nqn             string `json:"nqn"`
	Subtype         string `json:"subtype"`
	ListenAddresses []struct {
		Trtype  string `json:"trtype"`
		Adrfam  string `json:"adrfam"`
		TrAddr  string `json:"traddr"`
		TrSvcId string `json:trsvcid`
	} `json:"listen_addresses"`
	Hosts []struct {
		Nqn string `json:"nqn"`
	} `json:"hosts"`
	AllowAnyHost bool   `json:"allow_any_host"`
	SerialNumber string `json:"serial_number"`
	ModelNumber  string `json:"model_number"`
	Namespaces   []struct {
		Nsid int    `json:"nsid"`
		name string `json:"name"`
	} `json:"namespaces"`
}

type OperationClient struct {
	sc         *SpdkClient
	nameToBdev map[string]*bdevConf
	nqnToNvmf  map[string]*nvmfConf
}

type spdkErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (oc *OperationClient) BdevGetIosat(bdevName string) (*Iostat, error) {
	params := &struct {
		Name string `json:"name"`
	}{
		Name: bdevName,
	}
	rsp := &struct {
		Error  *spdkErr `json:"error"`
		Result *struct {
			TickRate uint64 `json:"tick_rate"`
			Bdevs    []struct {
				BytesRead               uint64 `json:"bytes_read"`
				NumReadOps              uint64 `json:"num_read_ops"`
				BytesWritten            uint64 `json:"bytes_written"`
				NumWriteOps             uint64 `json:"num_write_ops"`
				ByetsUnampped           uint64 `json:"num_unmap_ops"`
				NumUnmapOps             uint64 `json:"num_unmap_ops"`
				ReadLatencyTicks        uint64 `json:"read_latency_ticks"`
				WriteLatencyTicks       uint64 `json:"write_latency_ticks"`
				UnmapLatencyTicks       uint64 `json:"unmap_latency_ticks"`
				QueueDepthPollingPeriod uint64 `json:"queue_depth_polling_period"`
				QueueDepth              uint64 `json:"queue_depth"`
				IoTime                  uint64 `json:"io_time"`
				WeightedIoTime          uint64 `json:"WeightedIoTime"`
			}
		}
	}{}
	err := oc.sc.Invoke("bdev_get_iostat", params, rsp)
	if err != nil {
		logger.Error("bdev_get_iostat failed: %v", err)
		return nil, err
	}
	if rsp.Error != nil {
		logger.Error("bdev_get_iostat rsp err: %v", *rsp.Error)
		return nil, fmt.Errorf("bdev_get_iostat rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	if rsp.Result == nil {
		logger.Error("bdev_get_iostat result is nil")
		return nil, fmt.Errorf("bdev_get_iostat result is nil")
	}
	cnt := len(rsp.Result.Bdevs)
	if cnt != 1 {
		return nil, fmt.Errorf("bdev_get_iostat invalid cnt: %d", cnt)
	}
	return &Iostat{
		TickRate:                rsp.Result.TickRate,
		BytesRead:               rsp.Result.Bdevs[0].BytesRead,
		NumReadOps:              rsp.Result.Bdevs[0].NumReadOps,
		BytesWritten:            rsp.Result.Bdevs[0].BytesWritten,
		NumWriteOps:             rsp.Result.Bdevs[0].NumWriteOps,
		ByetsUnampped:           rsp.Result.Bdevs[0].ByetsUnampped,
		NumUnmapOps:             rsp.Result.Bdevs[0].NumUnmapOps,
		ReadLatencyTicks:        rsp.Result.Bdevs[0].ReadLatencyTicks,
		WriteLatencyTicks:       rsp.Result.Bdevs[0].WriteLatencyTicks,
		UnmapLatencyTicks:       rsp.Result.Bdevs[0].UnmapLatencyTicks,
		QueueDepthPollingPeriod: rsp.Result.Bdevs[0].QueueDepthPollingPeriod,
		QueueDepth:              rsp.Result.Bdevs[0].QueueDepth,
		IoTime:                  rsp.Result.Bdevs[0].IoTime,
		WeightedIoTime:          rsp.Result.Bdevs[0].WeightedIoTime,
	}, nil
}

func (oc *OperationClient) LoadNvmfs() error {
	rsp := &struct {
		Error  *spdkErr     `json:"error"`
		Result *[]*nvmfConf `json:"result"`
	}{}
	err := oc.sc.Invoke("nvmf_get_subsystems", nil, rsp)
	if err != nil {
		logger.Error("nvmf_get_subsystems failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("nvmf_get_subsystems rsp err: %v", *rsp.Error)
		return fmt.Errorf("nvmf_get_subsystems rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	if rsp.Result == nil {
		logger.Error("nvmf_get_subsystems result is nil")
		return fmt.Errorf("nvmf_get_subsystems result is nil")
	}
	for _, nvmf := range *rsp.Result {
		oc.nqnToNvmf[nvmf.Nqn] = nvmf
	}
	return nil
}

func (oc *OperationClient) LoadBdevs() error {
	rsp := &struct {
		Error  *spdkErr     `json:"error"`
		Result *[]*bdevConf `json"result"`
	}{}
	err := oc.sc.Invoke("bdev_get_bdevs", nil, rsp)
	if err != nil {
		logger.Error("bdev_get_bdevs failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("bdev_get_bdevs rsp err: %v", *rsp.Error)
		return fmt.Errorf("bdev_get_bdevs rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	if rsp.Result == nil {
		logger.Error("bdev_get_bdevs result is nil")
		return fmt.Errorf("bdev_get_bdevs result is nil")
	}
	for _, bdev := range *rsp.Result {
		oc.nameToBdev[bdev.name] = bdev
	}
	return nil
}

func (oc *OperationClient) setQosLimit(name string,
	rwIosPerSec, rwMbytesPerSec, rMbytesPerSec, wMbytesPerSec uint64) error {
	params := &struct {
		Name           string `json:"name"`
		RwIosPerSec    uint64 `json:"rw_ios_per_sec"`
		RwMbytesPerSec uint64 `json:"rw_mbytes_per_sec"`
		RMbytesPerSec  uint64 `json:"r_mbytes_per_sec"`
		WMbytesPerSec  uint64 `json:"w_mbytes_per_sec"`
	}{
		Name:           name,
		RwIosPerSec:    rwIosPerSec,
		RwMbytesPerSec: rwMbytesPerSec,
		RMbytesPerSec:  rMbytesPerSec,
		WMbytesPerSec:  wMbytesPerSec,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err := oc.sc.Invoke("bdev_set_qos_limit", params, rsp)
	if err != nil {
		logger.Error("bdev_set_qos_limit failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("bdev_set_qos_limit rsp err: %v", *rsp.Error)
		return fmt.Errorf("bdev_set_qos_limit rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) getBdevByPrefix(prefix string) ([]string, error) {
	beLvolList := make([]string, 0)
	for _, bdev := range oc.nameToBdev {
		for _, alias := range bdev.aliases {
			if strings.HasPrefix(alias, prefix) {
				beLvolList = append(beLvolList, alias)
				break
			}
		}
	}
	return beLvolList, nil
}

func (oc *OperationClient) GetBeNqnList(prefix string) ([]string, error) {
	beNqnList := make([]string, 0)
	for nqn, _ := range oc.nqnToNvmf {
		if strings.HasPrefix(nqn, prefix) {
			beNqnList = append(beNqnList, nqn)
		}
	}
	return beNqnList, nil
}

func (oc *OperationClient) createNvmfSubsystem(nqn string) error {
	params := &struct {
		Nqn          string `json:"nqn"`
		AllowAnyHost bool   `json:"allow_any_host"`
		SerialNumber string `json:"serial_number"`
		ModelNumber  string `json:"model_number"`
	}{
		Nqn:          nqn,
		AllowAnyHost: false,
		SerialNumber: NvmfSerailNumber(nqn),
		ModelNumber:  NVMF_MODULE_NUMBER,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err := oc.sc.Invoke("nvmf_create_subsystem", params, rsp)
	if err != nil {
		logger.Error("nvmf_create_subsystem failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("nvmf_create_subsystem rsp err: %v", *rsp.Error)
		return fmt.Errorf("nvmf_create_subsystem rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) createNvmfNs(nqn, bdevName string) error {
	id, err := NvmfUuid(nqn)
	if err != nil {
		logger.Error("can not generate nvmf uuid: %v", err)
		return err
	}
	params := &struct {
		Nqn       string `json:"nqn"`
		Namespace struct {
			Nsid     int    `json:"nsid"`
			Uuid     string `json:"uuid"`
			BdevName string `json:"bdev_name"`
		} `json:"namespace"`
	}{
		Nqn: nqn,
		Namespace: struct {
			Nsid     int    `json:"nsid"`
			Uuid     string `json:"uuid"`
			BdevName string `json:"bdev_name"`
		}{
			Nsid:     1,
			Uuid:     id,
			BdevName: bdevName,
		},
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err = oc.sc.Invoke("nvmf_subsystem_add_ns", params, rsp)
	if err != nil {
		logger.Error("nvmf_subsystem_add_ns failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("nvmf_subsystem_add_ns rsp err: %v", *rsp.Error)
		return fmt.Errorf("nvmf_subsystem_add_ns r sp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) createNvmfListener(nqn string, lisConf *LisConf) error {
	params := &struct {
		Nqn           string `json:"nqn"`
		ListenAddress struct {
			TrType  string `json:"trtype"`
			TrAddr  string `json:"traddr"`
			AdrFam  string `json:"adrfam"`
			TrSvcId string `json:"trsvcid"`
		}
	}{
		Nqn: nqn,
		ListenAddress: struct {
			TrType  string `json:"trtype"`
			TrAddr  string `json:"traddr"`
			AdrFam  string `json:"adrfam"`
			TrSvcId string `json:"trsvcid"`
		}{
			TrType:  lisConf.TrType,
			TrAddr:  lisConf.TrAddr,
			AdrFam:  lisConf.AdrFam,
			TrSvcId: lisConf.TrSvcId,
		},
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err := oc.sc.Invoke("nvmf_subsystem_add_listener", params, rsp)
	if err != nil {
		logger.Error("nvmf_subsystem_add_listener failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("nvmf_subsystem_add_listener rsp err: %v", *rsp.Error)
		return fmt.Errorf("nvmf_subsystem_add_listener rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) createNvmfHost(nqn, host string) error {
	params := &struct {
		Nqn  string `json:"nqn"`
		Host string `json:"host"`
	}{
		Nqn:  nqn,
		Host: host,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err := oc.sc.Invoke("nvmf_subsystem_add_host", params, rsp)
	if err != nil {
		logger.Error("nvmf_subsystem_add_host failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("nvmf_subsystem_add_host rsp err: %v", *rsp.Error)
		return fmt.Errorf("nvmf_subsystem_add_host rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) deleteNvmfHost(nqn, host string) error {
	params := &struct {
		Nqn  string `json:"nqn"`
		Host string `json:"host"`
	}{
		Nqn:  nqn,
		Host: host,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err := oc.sc.Invoke("nvmf_subsystem_remove_host", params, rsp)
	if err != nil {
		logger.Error("nvmf_subsystem_remove_host failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("nvmf_subsystem_remove_host rsp err: %v", *rsp.Error)
		return fmt.Errorf("nvmf_subsystem_remove_host rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) deleteNvmf(nqn string) error {
	params := &struct {
		Nqn string `json:"nqn"`
	}{
		Nqn: nqn,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err := oc.sc.Invoke("nvmf_delete_subsystem", params, rsp)
	if err != nil {
		logger.Error("nvmf_delete_subsystem failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("nvmf_delete_subsystem rsp err: %v", *rsp.Error)
		return fmt.Errorf("nvmf_delete_subsystem rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) CreateBeNvmf(beNqnName, beLvolFullName, feNqnName string,
	lisConf *LisConf) error {
	nvmf, ok := oc.nqnToNvmf[beNqnName]
	// The nvmf subsystem should only allow frontend_nqn_name to access it.
	// If the allowed host nqn does not match frontend_nqn_name,
	// we should delete the subsystem and create a new one.
	// Because if we do not delete the subsystem,
	// the old host will still be able to access this subsystem
	// even we delete it from the nvmf_conf["host"]
	if ok && (len(nvmf.Hosts) > 0) && (nvmf.Hosts[0].Nqn != feNqnName) {
		err := oc.deleteNvmf(beNqnName)
		if err != nil {
			return err
		}
		nvmf = nil
	}
	if nvmf == nil {
		if err := oc.createNvmfSubsystem(beNqnName); err != nil {
			return err
		}
		if err := oc.createNvmfNs(beNqnName, beLvolFullName); err != nil {
			return err
		}
		if err := oc.createNvmfListener(beNqnName, lisConf); err != nil {
			return err
		}
		if err := oc.createNvmfHost(beNqnName, feNqnName); err != nil {
			return err
		}
	} else {
		if len(nvmf.Namespaces) == 0 {
			if err := oc.createNvmfNs(beNqnName, beLvolFullName); err != nil {
				return err
			}
		}
		if len(nvmf.ListenAddresses) == 0 {
			if err := oc.createNvmfListener(beNqnName, lisConf); err != nil {
				return err
			}
		}
		if len(nvmf.Hosts) == 0 {
			if err := oc.createNvmfHost(beNqnName, feNqnName); err != nil {
				return err
			}
		}
	}
	return nil
}

func (oc *OperationClient) DeleteBeNvmf(nqn string) error {
	return oc.deleteNvmf(nqn)
}

func (oc *OperationClient) GetBeLvolList(prefix string) ([]string, error) {
	return oc.getBdevByPrefix(prefix)
}

func (oc *OperationClient) CreateBeLvol(lvsName, lvolName string, size uint64,
	rwIosPerSec, rwMbytesPerSec, rMbytesPerSec, wMbytesPerSec uint64) error {
	fullName := fmt.Sprintf("%s/%s", lvsName, lvolName)
	exist, err := oc.bdevExist(fullName)
	if err != nil {
		return err
	}
	if !exist {
		params := &struct {
			LvolName      string `json:"lvol_name"`
			Size          uint64 `json:"size"`
			LvsName       string `json:"lvs_name"`
			ClearMethod   string `json:"clear_method"`
			ThinProvision bool   `json:"thin_provision"`
		}{
			LvolName:      lvolName,
			Size:          size,
			LvsName:       lvsName,
			ClearMethod:   "none",
			ThinProvision: false,
		}
		rsp := &struct {
			Error *spdkErr `json:"error"`
		}{}
		err = oc.sc.Invoke("bdev_lvol_create", params, rsp)
		if err != nil {
			logger.Error("bdev_lvol_delete failed: %v", err)
			return err
		}
		if rsp.Error != nil {
			logger.Error("bdev_lvol_delete rsp err: %v", *rsp.Error)
			return fmt.Errorf("bdev_lvol_delete rsp err: %d %s",
				rsp.Error.Code, rsp.Error.Message)
		}
	}
	return oc.setQosLimit(fullName, rwIosPerSec, rwMbytesPerSec, rMbytesPerSec, wMbytesPerSec)
}

func (oc *OperationClient) DeleteBeLvol(name string) error {
	params := &struct {
		Name string `json:"name"`
	}{
		Name: name,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err := oc.sc.Invoke("bdev_lvol_delete", params, rsp)
	if err != nil {
		logger.Error("bdev_lvol_delete failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("bdev_lvol_delete rsp err: %v", *rsp.Error)
		return fmt.Errorf("bdev_lvol_delete rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) GetPdLvsList(prefix string) ([]string, error) {
	rsp := &struct {
		Error  *spdkErr `json:"error"`
		Result *[]*struct {
			Name string `json:"name"`
		}
	}{}
	err := oc.sc.Invoke("bdev_lvol_get_lvstores", nil, rsp)
	if err != nil {
		logger.Error("bdev_lvol_get_lvstores failed: %v", err)
		return nil, err
	}
	if rsp.Error != nil {
		logger.Error("bdev_lvol_get_lvstores rsp err: %v", *rsp.Error)
		return nil, fmt.Errorf("bdev_lvol_get_lvstores rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	if rsp.Result == nil {
		logger.Error("bdev_lvol_get_lvstores result is nil")
		return nil, fmt.Errorf("bdev_lvol_get_lvstores result is nil")
	}
	pdLvsList := make([]string, 0)
	for _, lvs := range *rsp.Result {
		if strings.HasPrefix(lvs.Name, prefix) {
			pdLvsList = append(pdLvsList, lvs.Name)
		}
	}
	return pdLvsList, nil
}

func (oc *OperationClient) GetLvsInfo(lvsName string) (*LvsInfo, error) {
	params := &struct {
		LvsName string `json:"lvs_name"`
	}{
		LvsName: lvsName,
	}
	rsp := &struct {
		Error  *spdkErr `json:"error"`
		Result *[]*struct {
			Uuid              string `json:"uuid"`
			BaseBdev          string `json:"base_bdev"`
			FreeClusters      uint64 `json:"free_clusters"`
			ClusterSize       uint64 `json:"cluster_size"`
			TotalDataClusters uint64 `json:"total_data_cluster"`
			BlockSize         uint64 `json:"block_size"`
			Name              string `json:"name"`
		}
	}{}
	err := oc.sc.Invoke("bdev_lvol_get_lvstores", params, rsp)
	if err != nil {
		logger.Error("bdev_lvol_get_lvstores failed: %v", err)
		return nil, err
	}
	if rsp.Error != nil {
		logger.Error("bdev_lvol_get_lvstores rsp err: %v", *rsp.Error)
		return nil, fmt.Errorf("bdev_lvol_get_lvstores rsp err %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	if rsp.Result == nil {
		logger.Error("bdev_lvol_get_lvstores result is nil")
		return nil, fmt.Errorf("bdev_lvol_get_lvstores result is nil")
	}
	cnt := len(*rsp.Result)
	if cnt != 1 {
		return nil, fmt.Errorf("bdev_lvol_get_lvstores invalid cnt: %d", cnt)
	}
	return &LvsInfo{
		FreeClusters:      (*rsp.Result)[0].FreeClusters,
		ClusterSize:       (*rsp.Result)[0].ClusterSize,
		TotalDataClusters: (*rsp.Result)[0].TotalDataClusters,
		BlockSize:         (*rsp.Result)[0].BlockSize,
	}, nil
}

func (oc *OperationClient) lvsExist(lvsName string) (bool, error) {
	params := &struct {
		LvsName string `json:"lvs_name"`
	}{
		LvsName: lvsName,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err := oc.sc.Invoke("bdev_lvol_get_lvstores", params, rsp)
	if err != nil {
		logger.Error("bdev_lvol_get_lvstores failed: %v", err)
		return false, err
	}
	if rsp.Error == nil {
		return true, nil
	} else {
		return false, nil
	}
}
func (oc *OperationClient) CreatePdLvs(pdLvsName, bdevName string) error {
	exist, err := oc.lvsExist(pdLvsName)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}

	params := &struct {
		LvsName     string `json:"lvs_name"`
		BdevName    string `json:"bdev_name"`
		ClearMethod string `json:"clear_method"`
		ClusterSz   uint64 `json:"cluster_sz"`
	}{
		LvsName:     pdLvsName,
		BdevName:    bdevName,
		ClearMethod: "none",
		ClusterSz:   CLUSTER_SIZE,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err = oc.sc.Invoke("bdev_lvol_create_lvstore", params, rsp)
	if err != nil {
		logger.Error("bdev_lvol_create_lvstore failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("bdev_lvol_create_lvstore rsp err: %v", *rsp.Error)
		return fmt.Errorf("bdev_lvol_create_lvstore rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) DeletePdLvs(lvsName string) error {
	params := &struct {
		LvsName string `json:"lvs_name"`
	}{
		LvsName: lvsName,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err := oc.sc.Invoke("bdev_lvol_delete_lvstore", params, rsp)
	if err != nil {
		logger.Error("bdev_lvol_delete_lvstore failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("bdev_lvol_delete_lvstore rsp err: %v", *rsp.Error)
		return fmt.Errorf("bdev_lvol_delete_lvstore rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) GetPdBdevList(prefix string) ([]string, error) {
	return oc.getBdevByPrefix(prefix)
}

func (oc *OperationClient) bdevExist(name string) (bool, error) {
	params := &struct {
		Name string `json:"name"`
	}{
		Name: name,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err := oc.sc.Invoke("bdev_get_bdevs", params, rsp)
	if err != nil {
		logger.Error("bdev_get_bdevs failed: %v", err)
		return false, err
	}
	if rsp.Error == nil {
		return true, nil
	} else {
		return false, nil
	}
}

func (oc *OperationClient) CreatePdMalloc(name string, size uint64) error {
	exist, err := oc.bdevExist(name)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}

	blockSize := SPDK_PAGE_SIZE
	numBlocks := (size + blockSize - 1) / blockSize
	params := &struct {
		Name      string `json:"name"`
		BlockSize uint64 `json:"block_size"`
		NumBlocks uint64
	}{
		Name:      name,
		BlockSize: blockSize,
		NumBlocks: numBlocks,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err = oc.sc.Invoke("bdev_malloc_create", params, rsp)
	if err != nil {
		logger.Error("bdev_malloc_create failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("bdev_malloc_create rsp err: %v", *rsp.Error)
		return fmt.Errorf("bdev_malloc_create rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) deletePdMalloc(bdevName string) error {
	params := &struct {
		Name string `json:"name"`
	}{
		Name: bdevName,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err := oc.sc.Invoke("bdev_malloc_delete", params, rsp)
	if err != nil {
		logger.Error("bdev_malloc_delete failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("bdev_malloc_delete rsp err: %v", *rsp.Error)
		return fmt.Errorf("bdev_malloc_delete rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) CreatePdAio(name string, fileName string) error {
	exist, err := oc.bdevExist(name)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}

	blockSize := SPDK_PAGE_SIZE
	params := &struct {
		Name      string `json:"name"`
		BlockSize uint64 `json:"block_size"`
		FileName  string `json:"filename"`
	}{
		Name:      name,
		BlockSize: blockSize,
		FileName:  fileName,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err = oc.sc.Invoke("bdev_aio_create", params, rsp)
	if err != nil {
		logger.Error("bdev_aio_create failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("bdev_aio_create rsp err: %v", *rsp.Error)
		return fmt.Errorf("bdev_aio_create rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) deletePdAio(bdevName string) error {
	params := &struct {
		Name string `json:"name"`
	}{
		Name: bdevName,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err := oc.sc.Invoke("bdev_aio_delete", params, rsp)
	if err != nil {
		logger.Error("bdev_aio_delete failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("bdev_aio_delete rsp err: %v", *rsp.Error)
		return fmt.Errorf("bdev_aio_delete r sp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) CreatePdNvme(name, realName, trAddr string) error {
	exist, err := oc.bdevExist(realName)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}

	params := &struct {
		Name   string `json:"name"`
		TrType string `json:"trtype"`
		TrAddr string `json:"traddr"`
	}{
		Name:   name,
		TrType: "pcie",
		TrAddr: trAddr,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err = oc.sc.Invoke("bdev_nvme_attach_controller", params, rsp)
	if err != nil {
		logger.Error("bdev_nvme_attach_controller failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("bdev_nvme_attach_controller rsp err: %v", *rsp.Error)
		return fmt.Errorf("bdev_nvme_attach_controller rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) deletePdNvme(bdevName string) error {
	if !strings.HasSuffix(bdevName, "n1") {
		logger.Error("invalid nvme bdev name: %s", bdevName)
		return fmt.Errorf("invalid nvme bdev name: %s", bdevName)
	}
	params := &struct {
		Name string `json:"name"`
	}{
		Name: bdevName[:len(bdevName)-2],
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err := oc.sc.Invoke("bdev_nvme_detach_controller", params, rsp)
	if err != nil {
		logger.Error("bdev_nvme_detach_controller failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("bdev_nvme_detach_controller rsp err: %v", *rsp.Error)
		return fmt.Errorf("bdev_nvme_detach_controller rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) DeletePdBdev(bdevName string) error {
	bdev, ok := oc.nameToBdev[bdevName]
	if !ok {
		return fmt.Errorf("Unknow bdev: %s", bdevName)
	}
	switch productName := bdev.productName; productName {
	case "Malloc disk":
		return oc.deletePdMalloc(bdevName)
	case "AIO disk":
		return oc.deletePdAio(bdevName)
	case "NVMe disk":
		return oc.deletePdNvme(bdevName)
	default:
		return fmt.Errorf("unknow productName: %s", productName)
	}
}

func (oc *OperationClient) ExamineBdev(bdevName string) error {
	params := &struct {
		Name string `json:"name"`
	}{
		Name: bdevName,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err := oc.sc.Invoke("bdev_examine", params, rsp)
	if err != nil {
		logger.Error("bdev_examine failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("bdev_examine rsp err: %v", *rsp.Error)
		return fmt.Errorf("bdev_examine rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func (oc *OperationClient) EnableHistogram(bdevName string) error {
	params := &struct {
		Name   string `json:"name"`
		Enable bool   `json:"enable"`
	}{
		Name:   bdevName,
		Enable: true,
	}
	rsp := &struct {
		Error *spdkErr `json:"error"`
	}{}
	err := oc.sc.Invoke("bdev_enable_histogram", params, rsp)
	if err != nil {
		logger.Error("bdev_enable_histogram failed: %v", err)
		return err
	}
	if rsp.Error != nil {
		logger.Error("bdev_enable_histogram rsp err: %v", *rsp.Error)
		return fmt.Errorf("bdev_enable_histogram rsp err: %d %s",
			rsp.Error.Code, rsp.Error.Message)
	}
	return nil
}

func NewOperationClient(sc *SpdkClient) *OperationClient {
	return &OperationClient{
		sc:         sc,
		nameToBdev: make(map[string]*bdevConf),
		nqnToNvmf:  make(map[string]*nvmfConf),
	}
}
