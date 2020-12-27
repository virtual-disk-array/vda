package mockspdk

import (
	"encoding/json"
	"net"
	"os"
	"sync"
	"testing"
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
	address   string
	wg        sync.WaitGroup
}

func defaultMethod(params interface{}) (*SpdkErr, interface{}) {
	return nil, true
}

func (s *MockSpdkServer) AddMethod(name string,
	api func(params interface{}) (*SpdkErr, interface{})) {
	s.nameToApi[name] = api
}

func (s *MockSpdkServer) handleApi(req *spdkReq) *spdkRsp {
	api, ok := s.nameToApi[req.Method]
	if !ok {
		api = defaultMethod
	}
	spdkErr, result := api(req.Params)
	rsp := &spdkRsp{
		JsonRpc: req.JsonRpc,
		Id:      req.Id,
		Error:   spdkErr,
		Result:  result,
	}
	return rsp
}

func (s *MockSpdkServer) handleConn(conn net.Conn) {
	defer conn.Close()
	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)
	for {
		req := &spdkReq{}
		if err := dec.Decode(req); err != nil {
			s.t.Logf("Decode err: %v", err)
			return
		}
		rsp := s.handleApi(req)
		if err := enc.Encode(rsp); err != nil {
			s.t.Logf("Encode err: %v", err)
			return
		}
	}
}

func (s *MockSpdkServer) run() {
	defer s.wg.Done()
	for {
		conn, err := s.lis.Accept()
		if err != nil {
			s.t.Logf("MockSpdkServer accept err: %v", err)
			return
		}
		s.handleConn(conn)
	}
}

func (s *MockSpdkServer) Run() {
	s.wg.Add(1)
	go s.run()
}

func (s *MockSpdkServer) Stop() {
	s.lis.Close()
	s.wg.Wait()
	os.Remove(s.address)
}

func NewMockSpdkServer(network, address string, t *testing.T) (*MockSpdkServer, error) {
	os.Remove(address)
	lis, err := net.Listen(network, address)
	if err != nil {
		t.Errorf("MockSpdkServer listen failed: %v", err)
		return nil, err
	}
	s := &MockSpdkServer{
		t:         t,
		nameToApi: make(map[string]func(params interface{}) (*SpdkErr, interface{})),
		lis:       lis,
		address:   address,
	}
	return s, nil
}
