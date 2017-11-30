package models

import (
	"database/sql"
	"fmt"
	"github.com/kshvakov/clickhouse"
	"log"
	"github.com/go-redis/redis"
)

func NewClick(config string) (*sql.DB) {
	db, err := sql.Open("clickhouse", config)
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
	}
	return db
}

func NewRedis(addr string,password string) (*redis.Client) {
	db := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "", // no password set
		DB:       0,                         // use default DB
	})
	_, err := db.Ping().Result()
	if err != nil {
		log.Println(err)
	}
	return db
}

