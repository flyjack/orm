package orm

import (
	"errors"
	"reflect"
	"time"

	"github.com/garyburd/redigo/redis"
)

var Pool *redis.Pool = nil

func NewRedisCacheWithRedisPool(pool *redis.Pool) *RedisCache {
	return &RedisCache{pool}
}
func SetCacheWithPool(pool *redis.Pool) {
	cacheconn = &RedisCache{pool}
}

func GetCachePool() Cache {
	return cacheconn
}

// 新建 Redis 连接
func NewRedisCache(REDIS_HOST, PASSWD string) *RedisCache {
	Pool = &redis.Pool{
		MaxIdle:     30,               //最大的空闲连接数，表示即使没有redis连接时依然可以保持N个空闲的连接，而不被清除，随时处于待命状态。
		MaxActive:   300,              //最大的激活连接数，表示同时最多有N个连接
		IdleTimeout: 30 * time.Second, //最大的空闲连接等待时间，超过此时间后，空闲连接将被关闭
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", REDIS_HOST)
			if err != nil {
				return nil, err
			}
			if PASSWD != "" && len(PASSWD) > 0 {
				if _, err := c.Do("AUTH", PASSWD); err != nil {
					c.Close()
					return nil, err
				}
			}
			// 选择db
			c.Do("SELECT", cache_db)
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	return &RedisCache{Pool}
}

type RedisCache struct {
	*redis.Pool
}

func (c *RedisCache) ConnGet() redis.Conn {
	return c.Pool.Get()
}

func (c *RedisCache) Set(key string, b []byte) (err error) {
	conn := c.ConnGet()
	defer conn.Close()
	_, err = conn.Do("SET", key, b)
	return
}
func (c *RedisCache) Get(key string) ([]byte, error) {
	conn := c.ConnGet()
	defer conn.Close()
	return redis.Bytes(conn.Do("GET", key))
}
func (c *RedisCache) Keys(key string) (keys []string, err error) {
	conn := c.ConnGet()
	defer conn.Close()
	keys, err = redis.Strings(conn.Do("KEYS", key))
	return
}
func (c *RedisCache) Incrby(key string, n int64) (int64, error) {
	conn := c.ConnGet()
	defer conn.Close()
	if n == 0 {
		return redis.Int64(conn.Do("GET", key))
	}
	return redis.Int64(conn.Do("INCRBY", key, n))
}
func (c *RedisCache) Hset(key, field string, b []byte) (bool, error) {
	conn := c.ConnGet()
	defer conn.Close()
	_, err := conn.Do("HSET", key, field, b)
	if err != nil {
		return false, err
	} else {
		return true, nil
	}
}
func (c *RedisCache) Hmset(key string, maping interface{}) (err error) {

	switch maping.(type) {
	case map[string]interface{}:
		conn := c.ConnGet()
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
	conn := c.ConnGet()
	defer conn.Close()
	return redis.Bytes(conn.Do("HGET", key, field))
}
func (c *RedisCache) Hincrby(key, field string, n int64) (int64, error) {
	conn := c.ConnGet()
	defer conn.Close()
	if n == 0 {
		return redis.Int64(conn.Do("HGET", key, field))
	}
	return redis.Int64(conn.Do("HINCRBY", key, field, n))
}

func (c *RedisCache) Exists(key string) (bool, error) {
	conn := c.ConnGet()
	defer conn.Close()
	return redis.Bool(conn.Do("EXISTS", key))
}
func (c *RedisCache) Del(key string) (bool, error) {
	conn := c.ConnGet()
	defer conn.Close()
	return redis.Bool(conn.Do("DEL", key))
}

func (c *RedisCache) key2Mode(key string, typ reflect.Type, val reflect.Value) error {
	conn := c.ConnGet()
	defer conn.Close()
	conn.Send("MULTI")
	vals := []interface{}{}
	timeField := []int{}
	for i := 0; i < typ.NumField(); i++ {
		conn.Send("HGET", key, typ.Field(i).Name)
		switch val.Field(i).Interface().(type) {
		case time.Time:
			timeField = append(timeField, i)
			var str string
			vals = append(vals, &str)
		default:
			vals = append(vals, val.Field(i).Addr().Interface())
		}
	}

	reply, err := redis.Values(conn.Do("EXEC"))
	if err != nil {
		return err
	}
	if _, err := redis.Scan(reply, vals...); err == nil {
		var n int
		for _, n = range timeField {
			if time, e := time.Parse(time.RFC1123Z, string(reply[n].([]byte))); e == nil {
				val.Field(n).Set(reflect.ValueOf(time))
			}
		}
		return nil
	} else {
		return err
	}
}
