package orm

var cacheConsistent = NewConsistent()
var cacheconn Cache

//是否启用cache 的hash分布支持， 为不支持分布式集群的redis 2.x和其他cache缓存设置true
var use_hash_cache bool = false

func UseHashCache(b bool) {
	use_hash_cache = b
}

func AddCacheAddress(address, password string) {

	cacheconn = NewRedisCache(address, password)

}
func SetCacheAddress(keys []string, password string) {

	cacheconn = NewRedisCache(keys[0], password)

}

func DelCacheAddress(key string) {

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
	/*
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
		}*/
}

//通过一致性hash服务， 得到当前key应该分配给哪个redis服务器
func getCacheAddrByKey(key string) (string, error) {
	return cacheConsistent.Get(key)
}

func GetCacheClient(key string) Cache {
	if use_hash_cache == false {
		return cacheconn
	}
	return nil
}
