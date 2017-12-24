package lib

import (
	"database/sql"
	"github.com/go-redis/redis"
)

var (
	DBClickhouseBad      *sql.DB
	DBClickhouseGood     *sql.DB
	ConfigClickhouseBad  string
	ConfigClickhouseGood string
	DBRedisStat          *redis.Client
	DBRedisIp            *redis.Client
)
