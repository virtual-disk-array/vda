package mockspdk

import (
	"encoding/json"
	"io"
	"net"
	"os"
	"testing"
)

const (
	STOP_SERVER = "stop_server"
)

type spdkReq struct {
	JsonRpc string `json:"jsonrpc"`
	Method  string `json:"method"`
	Id      uint64 `json:"id"`
	Params  interface{}
}

type SpdkErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type spdkRsp struct {
	JsonRpc string      `json:"jsonrpc"`
	Id      uint64      `json:"id"`
	Error   *SpdkErr    `json:"error"`
	Result  interface{} `json:"result"`
}

type MockSpdkServer struct {
	t         *testing.T
	nameToApi map[string]func(params interface{}) (*SpdkErr, interface{})
	lis       net.Listener
	stop      bool
}

func (s *MockSpdkServer) addMethod(name string,
	api func(params interface{}) (*SpdkErr, interface{})) {
	s.nameToApi[name] = api
}

func (s *MockSpdkServer) handleApi(req *spdkReq) *spdkRsp {
	if req.Method == STOP_SERVER {
		s.stop = true
	}
	rsp := &spdkRsp{
		JsonRpc: req.JsonRpc,
		Id:      req.Id,
		Result:  true,
	}
	return rsp
}

func (s *MockSpdkServer) handleConn(conn net.Conn) {
	defer conn.Close()
	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)
	for {
		if s.stop {
			return
		}
		req := &spdkReq{}
		if err := dec.Decode(req); err != nil {
			if err == io.EOF {
				return
			} else {
				s.t.Error("MockSpdkServer conn decode err: %v", err)
				s.stop = true
				return
			}
		}
		rsp := s.handleApi(req)
		if err := enc.Encode(rsp); err != nil {
			if err == io.EOF {
				return
			} else {
				s.t.Error("MockSpdkServer conn encode err: %v", err)
			}
		}
	}
}

func (s *MockSpdkServer) Run() {
	defer s.lis.Close()
	for {
		if s.stop {
			return
		}
		conn, err := s.lis.Accept()
		if err != nil {
			s.t.Error("MockSpdkServer accept failed: %v", err)
			return
		}
		s.handleConn(conn)
	}
}

func NewMockSpdkServer(network, address string, t *testing.T) (*MockSpdkServer, error) {
	os.Remove(address)
	lis, err := net.Listen(network, address)
	if err != nil {
		t.Error("MockSpdkServer listen failed: %v", err)
		return nil, err
	}
	s := &MockSpdkServer{
		t:         t,
		nameToApi: make(map[string]func(params interface{}) (*SpdkErr, interface{})),
		lis:       lis,
		stop:      false,
	}
	return s, nil
}
