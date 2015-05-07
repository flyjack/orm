package orm

import (
	"errors"
	"time"

	"github.com/garyburd/redigo/redis"
)

func NewRedisCache(REDIS_HOST string) *RedisCache {
	client := &redis.Pool{
		MaxIdle:     5,                 //最大的空闲连接数，表示即使没有redis连接时依然可以保持N个空闲的连接，而不被清除，随时处于待命状态。
		MaxActive:   50,                //最大的激活连接数，表示同时最多有N个连接
		IdleTimeout: 180 * time.Second, //最大的空闲连接等待时间，超过此时间后，空闲连接将被关闭
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", REDIS_HOST)
			if err != nil {
				panic(err)
				return nil, err
			}
			// 选择db
			c.Do("SELECT", cache_db)
			return c, nil
		},
	}
	return &RedisCache{client}
}

type RedisCache struct {
	*redis.Pool
}

func (c *RedisCache) Set(key string, b []byte) (err error) {
	conn := c.Pool.Get()
	defer conn.Close()
	_, err = conn.Do("SET", key, b)
	return
}
func (c *RedisCache) Get(key string) ([]byte, error) {
	conn := c.Pool.Get()
	defer conn.Close()
	return redis.Bytes(conn.Do("GET", key))
}
func (c *RedisCache) Keys(key string) (keys []string, err error) {
	conn := c.Pool.Get()
	defer conn.Close()
	var k []interface{}
	k, err = redis.Values(conn.Do("KEYS", key))
	redis.Scan(k, keys)
	return
}
func (c *RedisCache) Incrby(key string, n int64) (int64, error) {
	conn := c.Pool.Get()
	defer conn.Close()
	return redis.Int64(conn.Do("INCRBY", key, n))
}
func (c *RedisCache) Hset(key, field string, b []byte) (bool, error) {
	conn := c.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("HSET", field, b)
	if err != nil {
		return false, err
	} else {
		return true, nil
	}
}
func (c *RedisCache) Hmset(key string, maping interface{}) (err error) {

	switch maping.(type) {
	case map[string]interface{}:
		conn := c.Pool.Get()
		defer conn.Close()
		conn.Do("MULTI")
		for k, v := range maping.(map[string]interface{}) {
			//setings = append(setings, k, v)
			conn.Do("HSET", key, k, v)
			//Debug.Println(key, k, v)
		}
		_, err = conn.Do("EXEC")
	default:
		Error.Println(err)
		err = errors.New("Hmset maping type error ")
	}
	return

}
func (c *RedisCache) Hget(key, field string) ([]byte, error) {
	conn := c.Pool.Get()
	defer conn.Close()
	return redis.Bytes(conn.Do("HGET", key, field))
}
func (c *RedisCache) Hincrby(key, field string, n int64) (int64, error) {
	conn := c.Pool.Get()
	defer conn.Close()
	return redis.Int64(conn.Do("HINCRBY", key, field, n))
}
func (c *RedisCache) Exists(key string) (bool, error) {
	conn := c.Pool.Get()
	defer conn.Close()
	return redis.Bool(conn.Do("EXISTS", key))
}
func (c *RedisCache) Del(key string) (bool, error) {
	conn := c.Pool.Get()
	defer conn.Close()
	return redis.Bool(conn.Do("DEL", key))
}
