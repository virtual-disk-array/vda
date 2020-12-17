package dnagent

import (
	"time"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/logger"
	pbdn "github.com/virtual-disk-array/vda/pkg/proto/dnagentapi"
)

type dnAgentServer struct {
	pbdn.UnimplementedDnAgentServer
	lisConf *lib.LisConf
	nf      *lib.NameFmt
	sc      *lib.SpdkClient
}

func newDnAgentServer(sockPath string, sockTimeout int,
	lisConf *lib.LisConf, trConf map[string]interface{}) *dnAgentServer {
	nf := lib.NewNameFmt(lib.DefaultVdaPrefix, lib.DefaultNqnPrefix)
	sc := lib.NewSpdkClient("unix", sockPath, time.Duration(sockTimeout)*time.Second)
	var rsp interface{}
	err := sc.Invoke("nvmf_create_transport", trConf, &rsp)
	if err != nil {
		logger.Fatal("Can not create nvmf transport: %v", err)
	}
	return &dnAgentServer{
		lisConf: lisConf,
		nf:      nf,
		sc:      sc,
	}
}
