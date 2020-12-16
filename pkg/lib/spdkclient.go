package lib

import (
	"encoding/json"
	"net"
	"sync"
	"time"

	"github.com/virtual-disk-array/vda/pkg/logger"
)

const (
	MaxReqId = 65536
)

type connInterceptor struct {
	conn net.Conn
}

func (ci connInterceptor) Read(b []byte) (n int, err error) {
	n, err = ci.conn.Read(b)
	if err != nil {
		logger.Error("SPDk read err: %v", err)
	} else {
		logger.Info("SPDK read bytes: %d", n)
		logger.Info("SPDK read data: %s", b)
	}
	return n, err
}

func (ci connInterceptor) Write(b []byte) (n int, err error) {
	logger.Info("SPDK write data: %s", b)
	n, err = ci.conn.Write(b)
	if err != nil {
		logger.Error("SPDK write err: %v", err)
	} else {
		logger.Info("SPDK write bytes: %d", n)
	}
	return n, err
}

type connCtx struct {
	ci    connInterceptor
	enc   *json.Encoder
	dec   *json.Decoder
	reqId int
}

type SpdkClient struct {
	connCtx *connCtx
	network string
	address string
	timeout time.Duration
	mu      sync.Mutex
}

func (sc *SpdkClient) openConn() error {
	conn, err := net.DialTimeout(sc.network, sc.address, sc.timeout)
	if err != nil {
		logger.Error("SPDK dial failed: %v %v %v %v",
			sc.network, sc.address, sc.timeout, err)
		return err
	}
	ci := connInterceptor{
		conn: conn,
	}
	sc.connCtx = &connCtx{
		ci:    ci,
		enc:   json.NewEncoder(ci),
		dec:   json.NewDecoder(ci),
		reqId: 0,
	}
	return nil
}

func (sc *SpdkClient) closeConn() {
	sc.connCtx.ci.conn.Close()
	sc.connCtx = nil
}

func (sc *SpdkClient) invoke(method string, params interface{},
	rsp interface{}) error {
	req := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  method,
		"id":      sc.connCtx.reqId,
	}
	sc.connCtx.reqId++
	if params != nil {
		req["params"] = params
	}
	if err := sc.connCtx.enc.Encode(&req); err != nil {
		logger.Error("SPDK encode err: %v", err)
		sc.closeConn()
		return err
	}
	if err := sc.connCtx.dec.Decode(rsp); err != nil {
		logger.Error("SPDK decode err: %v", err)
		sc.closeConn()
		return err
	}
	return nil
}

func (sc *SpdkClient) Invoke(method string, params interface{},
	rsp interface{}) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.connCtx != nil && sc.connCtx.reqId >= MaxReqId {
		sc.closeConn()
	}
	if sc.connCtx == nil {
		err := sc.openConn()
		if err != nil {
			return err
		}
	}
	return sc.invoke(method, params, rsp)
}

func NewSpdkClient(network, address string, timeout time.Duration) *SpdkClient {
	return &SpdkClient{
		connCtx: nil,
		network: network,
		address: address,
		timeout: timeout,
	}
}
