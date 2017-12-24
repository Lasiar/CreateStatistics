package models

import (
	"database/sql"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/kshvakov/clickhouse"
	"log"
	_ "github.com/lib/pq"
)

func NewClick(config string) *sql.DB {
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

func NewRedis(addr string, password string) *redis.Client {
	db := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password, // no password set
		DB:       0,  // use default DB
	})
	_, err := db.Ping().Result()
	if err != nil {
		log.Println(err)
	}
	return db
}

func NewPostSql(config string) *sql.DB {
	db, err := sql.Open("postgres", config)
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