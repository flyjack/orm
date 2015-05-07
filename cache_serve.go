package orm

import (
	"time"
)

var CacheConsistent = NewConsistent()

var (
	getCache = make(chan *comandGetCacheConn)

	updateCache = make(chan []string)
	CacheServer = map[string]Cache{}
)

//获取目标地址的功能
type comandGetCacheConn struct {
	Key  string
	Call chan Cache
}

func init() {
	go goCacheRuntime()

}

//守护服务
func goCacheRuntime() {
	for {
		select {
		case mapping := <-updateCache:
			CacheConsistent.Set(mapping)
		case t := <-getCache:
			addr, err := getCacheAddrByKey(t.Key)
			if err != nil {
				t.Call <- nil
				return
			}

			client, ok := CacheServer[addr]

			if !ok {
				client = NewRedisCache(addr)
				CacheServer[addr] = client
			}
			t.Call <- client

		}
	}
}

//通过一致性hash服务， 得到当前key应该分配给哪个redis服务器
func getCacheAddrByKey(key string) (string, error) {
	return CacheConsistent.Get(key)
}

func GetCacheClient(key string) Cache {
	p := new(comandGetCacheConn)
	p.Call = make(chan Cache, 1)
	p.Key = key
	getCache <- p
	select {
	case item := <-p.Call:
		return item
	case <-time.After(time.Second * 5):
		return nil
	}
}
