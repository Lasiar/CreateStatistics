package lib

type Json struct {
	Point      int             `json:"point"`
	Statistics [][]interface{} `json:"statistics"`
}

type Config struct {
	ClickhouseGood struct {
		Port   int    `json:"Port"`
		Addr   string `json:"Addr"`
		DbName string `json:"DbName"`
	} `json:"ClickhouseGood"`
	PostgresqlBad struct {
		User     string `json:"user"`
		Password string `json:"password"`
		DbName   string `json:"DbName"`
	} `json:"PostgresqlBad"`
	RedisStat struct {
		Addr     string `json:"addr"`
		Password string `json:"password"`
	} `json:"RedisStat"`
	RedisIP struct {
		Addr     string `json:"addr"`
		Password string `json:"password"`
	} `json:"RedisIp"`
	Port string `json:"Port"`
}