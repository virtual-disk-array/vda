package cnagent

import (
	"sync"
	"time"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbcn "github.com/virtual-disk-array/vda/pkg/proto/cnagentapi"
)

var (
	cnMutex     sync.Mutex
	lastVersion uint64
)

type cnAgentServer struct {
	pbcn.UnimplementedCnAgentServer
	lisConf *lib.LisConf
	nf      *lib.NameFmt
	sc      *lib.SpdkClient
}

func newCnAgentServer(sockPath string, sockTimeout int,
	lisConf *lib.LisConf, trConf map[string]interface{}) (*cnAgentServer, error) {
	nf := lib.NewNameFmt(lib.DefaultVdaPrefix, lib.DefaultNqnPrefix)
	sc := lib.NewSpdkClient("unix", sockPath, time.Duration(sockTimeout)*time.Second)
	var rsp interface{}
	err := sc.Invoke("nvmf_create_transport", trConf, &rsp)
	if err != nil {
		logger.Error("Can not create nvmf transport: %v", err)
		return nil, err
	}
	return &cnAgentServer{
		lisConf: lisConf,
		nf:      nf,
		sc:      sc,
	}, nil
}

func (cnAgent *cnAgentServer) Stop() {
	cnAgent.sc.Stop()
}
