package dnagent

import (
	"testing"
	"time"

	"github.com/virtual-disk-array/vda/pkg/lib"
	"github.com/virtual-disk-array/vda/pkg/mocks/mockspdk"
)

func Test1(t *testing.T) {
	s, err := mockspdk.NewMockSpdkServer("unix", "/tmp/t1.sock", t)
	if err != nil {
		return
	}
	sc := lib.NewSpdkClient("unix", "/tmp/t1.sock", time.Second)
	go s.Run()
	var rsp interface{}
	sc.Invoke(mockspdk.STOP_SERVER, nil, rsp)
}
