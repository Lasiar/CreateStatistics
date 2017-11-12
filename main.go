package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/kshvakov/clickhouse"
	"github.com/olekukonko/tablewriter"
	"github.com/satori/go.uuid"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"StatisticsCreate/lib"
)

var (
	dbClickhouseBad      *sql.DB
	dbClickhouseGood     *sql.DB
	configClickhouseBad  string
	configClickhouseGood string
	dbRedisStat          *redis.Client
	dbRedisIp            *redis.Client
	config               Config
)

type QueryClickhouse struct {
	point    int
	datetime int64
	md5      string
	len      int
}

type BadJson struct {
	ip   string
	json string
}

type Config struct {
	ClickhouseGood struct {
		Port   int    `json:"Port"`
		Addr   string `json:"Addr"`
		DbName string `json:"DbName"`
	} `json:"ClickhouseGood"`
	ClickhouseBad struct {
		Port   int    `json:"Port"`
		Addr   string `json:"Addr"`
		DbName string `json:"DbName"`
	} `json:"ClickhouseBad"`
	RedisStat struct {
		Addr     string `json:"addr"`
		Password string `json:"password"`
	} `json:"RedisStat"`
	RedisIP struct {
		Addr     string `json:"addr"`
		Password string `json:"password"`
	} `json:"RedisIp"`
}

const (
	dbClickhouseGoodQuery    = "INSERT INTO statistics(point_id, played, md5, len) VALUES (?, ?, toFixedString(?, 32),  ?)"
	dbClickhouseBadQuery = "INSERT INTO statistic(ip, json) VALUES (?, ?)"
)


func init() {
	var err error
	Configure()

	dbClickhouseBad, err = sql.Open("clickhouse", configClickhouseBad)
	if err != nil {
		log.Fatal(err)
	}
	if err := dbClickhouseBad.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
	}
	dbClickhouseGood, err = sql.Open("clickhouse", configClickhouseGood)
	if err != nil {
		log.Fatal(err)
	}
	if err := dbClickhouseGood.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
	}

	dbRedisStat = redis.NewClient(&redis.Options{
		Addr:     config.RedisStat.Addr,
		Password: config.RedisStat.Password, // no password set
		DB:       0,                                                                   // use default DB
	})
	_, err = dbRedisStat.Ping().Result()
	if err != nil {
		log.Println(err)
	}
	dbRedisIp = redis.NewClient(&redis.Options{
		Addr:     config.RedisIP.Addr,
		Password: config.RedisIP.Password	, // no password set
		DB:       0,                                                                   // use default DB
	})
	_, err = dbRedisIp.Ping().Result()
	if err != nil {
		log.Println(err)
	}
}

func Configure()  {
	file, err := ioutil.ReadFile("config/CreateStatistics.config")
	if err != nil {
		fmt.Println(err)
	}
	err = json.Unmarshal(file, &config)
	if err != nil {
		fmt.Println("Unmarshal config", err)
	}
	configClickhouseGood = fmt.Sprint("tcp://",config.ClickhouseGood.Addr,":",config.ClickhouseGood.Port,"?database=",config.ClickhouseGood.DbName,"&read_timeout=10&write_timeout=20")
	configClickhouseBad = fmt.Sprint("tcp://",config.ClickhouseBad.Addr,":",config.ClickhouseBad.Port,"?database=",config.ClickhouseBad.DbName,"&read_timeout=10&write_timeout=20")
	PrintConfig()
}

func main() {
	ticker := time.NewTicker(1 * time.Second)
	go ParseWithRedis(ticker.C)
	addr, err := determineListenAddress()
	http.HandleFunc("/gateway/statistics/create", httpServer)
	err = http.ListenAndServe(addr, nil) // set listen port
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func ParseWithRedis(ticker <-chan time.Time) {
	postArr := make(chan []string)
	for {
		select {
		case <-ticker:
			go prepareJson(postArr)
		case post := <-postArr:
			for _, p := range post {
				err := dbRedisStat.Del(p).Err()
				if err != nil {
					log.Println(err)
				}
			}
		}
	}
}
func prepareJson(postArr chan []string) {
	var (
		goodJson   []QueryClickhouse
		badJsonArr []BadJson
		badJson    BadJson
	)
	KeyDB, err := dbRedisStat.Keys("*ip:*").Result()

	if err != nil {
		log.Println(err)
	}
	if len(KeyDB) == 0 {
		return
	}
	valArr, err := dbRedisStat.MGet(KeyDB...).Result()
	if err != nil {
		log.Println(err)
	}
	for i, val := range valArr {
		d := strings.Index(KeyDB[i], "ip:")
		ip := KeyDB[i][d+3:]
		jsonRaw, err := validateTypeJson(val.(string))
		if err != nil {
			badJson.ip = ip
			badJson.json = val.(string)
			badJsonArr = append(badJsonArr, badJson)
			continue
		}
		q, err := jsonParser(jsonRaw)
		if err != nil {
			log.Println(err)
			continue
		}
		err = dbRedisIp.Set(strconv.Itoa(jsonRaw.Point), ip, 0).Err()
		if  err != nil {
			log.Println(err)
		}
		goodJson = append(goodJson, q...)

	}

	if len(badJsonArr) != 0 {
		err = sendToBadClick(badJsonArr)
		if err != nil {
			log.Println(err)
		}
	}
	if len(goodJson) != 0 {
		err = sendToClick(goodJson)
		if err != nil {
			log.Println(err)
			return
		}
	}
	postArr <- KeyDB
	return
}

func jsonParser(rawJson lib.Json) ([]QueryClickhouse, error) {
	var err error
	LenQuery := len(rawJson.Statistics)
	query := make([]QueryClickhouse, LenQuery, LenQuery)
	for p, first := range rawJson.Statistics {
		query[p].point = rawJson.Point
		for i, second := range first {
			switch i {
			case 0:
				query[p].datetime, err = strconv.ParseInt(second.(string), 10, 64)
				if err != nil {
					return nil, err
				}
			case 1:
				query[p].md5 = second.(string)
			case 2:
				query[p].len = int(second.(float64))
			}
		}
	}
	return query, nil
}


func validateTypeJson(jsonText string) (lib.Json, error) {
	var rawJson lib.Json
	err := json.Unmarshal([]byte(jsonText), &rawJson)
	if err != nil {
		return rawJson, err
	}
	if rawJson.Point == 0 {
		return rawJson, fmt.Errorf("WARNING: point == 0")
	}
	for _, first := range rawJson.Statistics {
		for i, second := range first {
			switch t := second.(type) {
			case float64:
				if i == 0 || i == 1 {
					return rawJson, fmt.Errorf("point %v WARNING: invalid json: want float 64, have %b", rawJson.Point, t)
				}
			case string:
				if i == 2 {
					return rawJson, fmt.Errorf("point %v WARNING: invalid json: want string, have %v", rawJson.Point, t)
				} else {
					if i == 1 {
						if strings.Contains(t, " ") {
							return rawJson, fmt.Errorf("point %v WARNING: invalid json: md5 has space", rawJson.Point)
						}
						if l := len(t); l != 32 {
							return rawJson, fmt.Errorf("point %v WARNING: invalid json: want md5 lenght 32, have %v", rawJson.Point, l)
						}
					} else {
						if strings.Contains(t, " ") {
							return rawJson, fmt.Errorf("point %v WARNING: invalid json: datetime has space", rawJson.Point)
						}
					}
				}
			default:
				return rawJson, fmt.Errorf("point %v WARNING: unknow type %v", rawJson.Point, t)
			}
		}
	}
	return rawJson, nil
}




func getRealAddr(r *http.Request) string {
	remoteIP := ""
	if parts := strings.Split(r.RemoteAddr, ":"); len(parts) == 2 {
		remoteIP = parts[0]
	}
	if xff := strings.Trim(r.Header.Get("X-Forwarded-For"), ","); len(xff) > 0 {
		addrs := strings.Split(xff, ",")
		lastFwd := addrs[len(addrs)-1]
		if ip := net.ParseIP(lastFwd); ip != nil {
			remoteIP = ip.String()
		}
	} else if xri := r.Header.Get("X-Real-Ip"); len(xri) > 0 {
		if ip := net.ParseIP(xri); ip != nil {
			remoteIP = ip.String()
		}
	}
	return remoteIP
}

func determineListenAddress() (string, error) {
	port := os.Getenv("PORT")
	if port == "" {
		return "", fmt.Errorf("$PORT not set")
	}
	return ":" + port, nil
}

func httpServer(w http.ResponseWriter, r *http.Request) {
	ip := getRealAddr(r)
	id := uuid.NewV4()
	data := r.PostFormValue("data")
	err := dbRedisStat.Set(fmt.Sprint(id, "_ip:", ip), data, 0).Err()
	if err != nil {
		log.Println(err)
		return
	}
	fmt.Fprint(w, `{"success":true}`)
}

func PrintConfig() {
	data := [][]string{
		[]string{"Cliclhouse Good addr", config.ClickhouseGood.Addr},
		[]string{"Clickhouse Good port", strconv.Itoa(config.ClickhouseGood.Port)},
		[]string{"Clickhouse Good DataBase", config.ClickhouseGood.DbName},
		[]string{"Cliclhouse Bad addr", config.ClickhouseBad.Addr},
		[]string{"Clickhouse Bad port", strconv.Itoa(config.ClickhouseBad.Port)},
		[]string{"Clickhouse Bad DataBase", config.ClickhouseBad.DbName},
		[]string{"Redis stat addr", config.RedisStat.Addr},
		[]string{"Redis stat password", config.RedisStat.Password},
		[]string{"Redis ip addr", config.RedisIP.Addr},
		[]string{"Redis ip password", config.RedisIP.Password},
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Key", "Value"})
	for _, v := range data {
		table.Append(v)
	}
	table.Render()
}


func sendToClick(array []QueryClickhouse) error {
	var (
		tx, _ = dbClickhouseGood.Begin()
	)
	stmt, err := tx.Prepare(dbClickhouseGoodQuery)
	if err != nil {
		log.Println(err)
	}
	for _, query := range array {
		if _, err := stmt.Exec(query.point,
			query.datetime,
			query.md5,
			query.len); err != nil {
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

func sendToBadClick(badJsons []BadJson) error {
	var (
		tx, _ = dbClickhouseBad.Begin()
	)
	stmt, err := tx.Prepare(dbClickhouseBadQuery)
	if err != nil {
		log.Println(err)
	}
	for _, query := range badJsons {
		if _, err := stmt.Exec(query.ip,
			query.json); err != nil {
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