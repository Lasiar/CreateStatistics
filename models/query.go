package models

import (
	"database/sql"
	"fmt"
	"github.com/kshvakov/clickhouse"
	"log"
	"github.com/go-redis/redis"
)

type QueryClickhouse struct {
	Point    int
	Datetime int64
	Md5      string
	Len      int
}

type BadJson struct {
	Ip   string
	Json interface{}
}

const (
	dbClickhouseGoodQuery = "INSERT INTO statistics(point_id, played, md5, len) VALUES (?, ?, toFixedString(?, 32),  ?)"
	dbClickhouseBadQuery  = `INSERT INTO badjson(ip, json) VALUES ($1, $2)`
)

func SendToClick(array []QueryClickhouse, db *sql.DB) error {
	if err := db.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			return fmt.Errorf("Error connect tp GoodClick: ", err)
		}
	}
	var (
		tx, _ = db.Begin()
	)
	stmt, err := tx.Prepare(dbClickhouseGoodQuery)
	if err != nil {
		log.Println(err)
	}
	for _, query := range array {
		if _, err := stmt.Exec(query.Point,
			query.Datetime,
			query.Md5,
			query.Len); err != nil {
			log.Println(err)
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func SendToBadClick(badJsons []BadJson, db *sql.DB) error {
	if err := db.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			return fmt.Errorf("Error connect tp BadClick: ", err)
		}
	}
	var (
		tx, _ = db.Begin()
	)
	stmt, err := tx.Prepare(dbClickhouseBadQuery)
	if err != nil {
		log.Println(err)
	}
	for _, query := range badJsons {
		if _, err := stmt.Exec(query.Ip,
			query.Json); err != nil {
			log.Println(err)
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func SendInfo(ip string, userAgent string, point string, db *redis.Client) {
	err := db.Set(fmt.Sprint(point, "_ip"), ip, 0).Err()
	if err != nil {
		log.Println(err)
	}
	err = db.Set(fmt.Sprint(point, "_user"), userAgent, 0).Err()
	if err != nil {
		log.Println(err)
	}
}