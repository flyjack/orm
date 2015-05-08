package orm

import (
	"time"
)

var cacheConsistent = NewConsistent()
var cacheconn Cache

//是否启用cache 的hash分布支持， 为不支持分布式集群的redis 2.x和其他cache缓存设置true
var use_hash_cache bool = true

func UseHashCache(b bool) {
	use_hash_cache = b
}

func AddCacheAddress(address string) {
	if use_hash_cache {

		cacheConsistent.Add(address)
	} else {
		cacheconn = NewRedisCache(address)
	}
}
func SetCacheAddress(keys []string) {

	if use_hash_cache {
		cacheConsistent.Set(keys)
	} else {
		cacheconn = NewRedisCache(keys[0])
	}
}
func DelCacheAddress(key string) {
	if use_hash_cache {
		cacheConsistent.Remove(key)
	}
}

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
	if use_hash_cache == false {
		return
	}
	for {
		select {
		case mapping := <-updateCache:
			cacheConsistent.Set(mapping)
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
	return cacheConsistent.Get(key)
}

func GetCacheClient(key string) Cache {
	if use_hash_cache == false {
		return cacheconn
	}
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
