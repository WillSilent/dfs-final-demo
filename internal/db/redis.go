package db

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

var (
	pool      *redis.Pool
	redisHost = "192.168.246.100:6379"
)

func newRedisPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     10, // 最大空闲连接数
		MaxActive:   0,  // 最大激活连接数（0 表示没有限制）
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			//此处对应redis ip及端口号
			conn, err := redis.Dial("tcp", redisHost)
			if err != nil {
				return nil, err
			}
			//此处1234对应redis密码
			if _, err := conn.Do("AUTH", "123456"); err != nil {
				conn.Close()
				return nil, err
			}
			return conn, err
		},
	}
}

func init() {
	pool = newRedisPool()
}

func RedisPool() *redis.Pool {
	return pool
}
