package lib

import (
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/virtual-disk-array/vda/pkg/logger"
)

type cacheItem struct {
	conn      *grpc.ClientConn
	timestamp int64
}

type GrpcCache struct {
	cache    map[string]cacheItem
	wg       sync.WaitGroup
	mu       sync.Mutex
	ttl      int64
	step     int
	interval int
	quit     chan bool
}

func (gc *GrpcCache) Get(sockAddr string) (*grpc.ClientConn, error) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	ci, ok := gc.cache[sockAddr]
	if ok {
		ci.timestamp = time.Now().Unix()
		return ci.conn, nil
	} else {
		conn, err := grpc.Dial(sockAddr, grpc.WithInsecure())
		if err != nil {
			logger.Error("grpc.Dial err: %v", err)
			return nil, err
		} else {
			ci := cacheItem{
				conn:      conn,
				timestamp: time.Now().Unix(),
			}
			gc.cache[sockAddr] = ci
			logger.Info("Create conn: %s", sockAddr)
			return ci.conn, nil
		}
	}
}

func (gc *GrpcCache) scan() {
	defer gc.wg.Done()
	count := 0
	now := time.Now().Unix()
	gc.mu.Lock()
	for {
		for sockAddr, ci := range gc.cache {
			count++
			if count <= gc.step {
				if now-ci.timestamp > gc.ttl {
					delete(gc.cache, sockAddr)
					ci.conn.Close()
					logger.Info("Close conn: %s", sockAddr)
				}
			} else {
				gc.mu.Unlock()
				count = 0
				select {
				case <-time.After(time.Duration(gc.interval) * time.Second):
					now = time.Now().Unix()
					gc.mu.Lock()
				case <-gc.quit:
					return
				}
			}
		}
		gc.mu.Unlock()
		count = 0
		select {
		case <-time.After(time.Duration(gc.interval) * time.Second):
			now = time.Now().Unix()
			gc.mu.Lock()
		case <-gc.quit:
			return
		}
	}
}

func (gc *GrpcCache) Close() {
	logger.Info("Closing GrpcCache")
	gc.mu.Lock()
	for sockAddr, ci := range gc.cache {
		delete(gc.cache, sockAddr)
		ci.conn.Close()
		logger.Info("Close conn: %s", sockAddr)
	}
	gc.mu.Unlock()
	close(gc.quit)
	gc.wg.Wait()
	logger.Info("Closed GrpcCache")
}

func NewGrpcCache(ttl int64, step int, interval int) *GrpcCache {
	gc := &GrpcCache{
		cache:    make(map[string]cacheItem),
		ttl:      ttl,
		step:     step,
		interval: interval,
		quit:     make(chan bool),
	}
	gc.wg.Add(1)
	go gc.scan()
	return gc
}
