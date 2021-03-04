package dnagent

import (
	"sync"
	"time"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbdn "github.com/virtual-disk-array/vda/pkg/proto/dnagentapi"
)

var (
	dnMutex     sync.Mutex
	lastVersion uint64
)

type dnAgentServer struct {
	pbdn.UnimplementedDnAgentServer
	lisConf *lib.LisConf
	nf      *lib.NameFmt
	sc      *lib.SpdkClient
}

func newDnAgentServer(sockPath string, sockTimeout int,
	lisConf *lib.LisConf, trConf map[string]interface{}) (*dnAgentServer, error) {
	nf := lib.NewNameFmt(lib.DefaultVdaPrefix, lib.DefaultNqnPrefix)
	sc := lib.NewSpdkClient("unix", sockPath, time.Duration(sockTimeout)*time.Second)
	var rsp interface{}
	err := sc.Invoke("nvmf_create_transport", trConf, &rsp)
	if err != nil {
		logger.Error("Can not create nvmf transport: %v", err)
		return nil, err
	}
	return &dnAgentServer{
		lisConf: lisConf,
		nf:      nf,
		sc:      sc,
	}, nil
}

func (dnAgent *dnAgentServer) Stop() {
	dnAgent.sc.Stop()
}
