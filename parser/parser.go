package parser

import (
	"CreateStatistics/lib"
	"CreateStatistics/models"
	"CreateStatistics/system"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"strconv"
	"strings"
	"time"
)

func PrepareJson(sendLog bool, dbRedis *redis.Client, dbRedisIp *redis.Client, dbClickhouseBad *sql.DB, dbClickhouseGood *sql.DB) {
	var (
		goodJson   []models.QueryClickhouse
		badJsonArr []models.BadJson
		badJson    models.BadJson
	)
	KeyDB, err := dbRedis.Keys("*ip:*").Result()
	if err != nil {
		log.Println(err)
	}
	if len(KeyDB) == 0 {
		return
	}
	valArr, err := dbRedis.MGet(KeyDB...).Result()
	if err != nil {
		log.Println(err)
		return
	}
	for i, val := range valArr {
		d := strings.Index(KeyDB[i], "ip:")
		u := strings.Index(KeyDB[i], "user_agent")
		ip := KeyDB[i][d+3 : u]
		userAgent := KeyDB[i][u+11:]
		valString, err := system.CheckString(val)
		if err != nil {
			log.Println(err)
			return
		}
		jsonRaw, err := validateTypeJson(valString)
		if err != nil {
			badJson.Ip = ip
			badJson.Json = val
			badJsonArr = append(badJsonArr, badJson)
			log.Println(err)
			continue
		}
		q, err := jsonParser(jsonRaw)
		if err != nil {
			log.Println("jsonParser", err)
			continue
		}
		point := strconv.Itoa(jsonRaw.Point)
		sendInfo(ip, userAgent, point, dbRedisIp)
		goodJson = append(goodJson, q...)
	}
	if len(badJsonArr) != 0 && sendLog {
		err := splitBadArrayJson(badJsonArr, dbClickhouseBad, 0)
		if err != nil {
			log.Println("Send to badJson: ", err)
			return
		}
	}
	if len(goodJson) != 0 {
		err := splitArrayJson(goodJson, dbClickhouseGood)
		if err != nil {
			log.Println("Send to stat: ", err)
			return
		}
	}
	err = dbRedis.Del(KeyDB...).Err()
	if err != nil {
		log.Println(err)
	}
	return
}

func jsonParser(rawJson lib.Json) ([]models.QueryClickhouse, error) {
	var err error
	LenQuery := len(rawJson.Statistics)
	query := make([]models.QueryClickhouse, LenQuery, LenQuery)
	for p, first := range rawJson.Statistics {
		query[p].Point = rawJson.Point
		for i, second := range first {
			switch i {
			case 0:
				query[p].Datetime, err = strconv.ParseInt(second.(string), 10, 64)
				if err != nil {
					return query, err
				}
			case 1:
				query[p].Md5 = second.(string)
			case 2:
				query[p].Len = int(second.(float64))
			}
		}
	}
	return query, nil
}

func validateTypeJson(jsonText interface{}) (lib.Json, error) {
	var rawJson lib.Json
	jsonString, err := system.CheckString(jsonText)
	if err != nil {
		log.Println(err)
		return rawJson, fmt.Errorf("type error: %v", rawJson)
	}
	err = json.Unmarshal([]byte(jsonString), &rawJson)
	if err != nil {
		return rawJson, fmt.Errorf("error json:", err, rawJson)
	}
	if rawJson.Point == 0 {
		return rawJson, fmt.Errorf("WARNING: point == 0")
	}
	if len(rawJson.Statistics) == 0 {
		return rawJson, fmt.Errorf("Corutpted json: %v", jsonText)
	}
	for _, first := range rawJson.Statistics {
		for i, second := range first {
			err := validStatisticJson(second, i)
			if err !=nil {
				return rawJson, fmt.Errorf("type error: %v JSONTEXT: %v", err, jsonText)
			}
		}
	}
	return rawJson, nil
}

func splitArrayJson(array []models.QueryClickhouse, dbClickhouseGood *sql.DB) error {
	time.Sleep(1 * time.Second)
	if len(array) >= 1000 {
		go splitArrayJson(array[900:], dbClickhouseGood)
		err := models.SendToClick(array[:900], dbClickhouseGood)
		if err != nil {
			return fmt.Errorf("Error statclick: ", err)
		}
	} else {
		err := models.SendToClick(array, dbClickhouseGood)
		if err != nil {
			log.Println("Error statclick: ", err)
			return err
			}
		return nil
	}
	return nil
}


func splitBadArrayJson(array []models.BadJson, dbClickhouseBad *sql.DB, i int) error {
	time.Sleep(1 * time.Second)
	if len(array) >= 1000 {
		go splitBadArrayJson(array[900:], dbClickhouseBad, i)
		err := models.SendToBadClick(array[:900], dbClickhouseBad)
		if err != nil {
			return fmt.Errorf("Error statclick: ", err)
		}
	} else {
		err := models.SendToBadClick(array, dbClickhouseBad)
		if err != nil {
			log.Println("Error statclick: ", err)
		}
		return nil
	}
	return nil
}

func sendInfo(ip string, userAgent string, point string, db *redis.Client) {
	err := db.Set(fmt.Sprint(point, "_ip"), ip, 0).Err()
	if err != nil {
		log.Println(err)
	}
	err = db.Set(fmt.Sprint(point, "_user"), userAgent, 0).Err()
	if err != nil {
		log.Println(err)
	}
}

func validStatisticJson(StatisticArr interface{}, iterator int) error {
	switch t := StatisticArr.(type) {
	case float64:
		if iterator == 0 || iterator == 1 {
			return fmt.Errorf("WARNING: invalid json: want float 64, have %v", t)
		}
	case string:
		if iterator == 2 {
			return fmt.Errorf("WARNING: invalid json: want string, have %v", t)
		} else {
			if iterator == 1 {
				if strings.Contains(t, " ") {
					return fmt.Errorf("WARNING: invalid json: md5 has space")
				}
				if l := len(t); l != 32 {
					return fmt.Errorf("WARNING: invalid json: want md5 lenght 32, have %v", t )
				}
			} else {
				if strings.Contains(t, " ") {
					return fmt.Errorf("WARNING: invalid json: datetime has space")
				}
			}
		}
	default:
		return fmt.Errorf("WARNING: unknow type %v", t)
	}
	return nil
}