package main

import (
	"CreateStatistics/lib"
	"CreateStatistics/models"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/kshvakov/clickhouse"
	"github.com/lazada/goprof"
	"github.com/olekukonko/tablewriter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/satori/go.uuid"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
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
	json interface{}
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
	Port string `json:"Port"`
}

const (
	dbClickhouseGoodQuery = "INSERT INTO statistics(point_id, played, md5, len) VALUES (?, ?, toFixedString(?, 32),  ?)"
	dbClickhouseBadQuery  = "INSERT INTO statistic(ip, json) VALUES (?, ?)"
)

func init() {
	Configure()
	dbClickhouseBad = models.NewClick(configClickhouseBad)
	dbClickhouseGood = models.NewClick(configClickhouseGood)
	dbRedisStat = models.NewRedis(config.RedisStat.Addr, config.RedisStat.Password)
	dbRedisStat = models.NewRedis(config.RedisIP.Addr, config.RedisIP.Password)

}

func Configure() {
	file, err := ioutil.ReadFile("config/CreateStatistics.config")
	if err != nil {
		fmt.Println(err)
	}
	err = json.Unmarshal(file, &config)
	if err != nil {
		fmt.Println("Unmarshal config", err)
	}
	configClickhouseGood = fmt.Sprint("tcp://", config.ClickhouseGood.Addr, ":", config.ClickhouseGood.Port, "?database=", config.ClickhouseGood.DbName, "&read_timeout=10&write_timeout=20")
	configClickhouseBad = fmt.Sprint("tcp://", config.ClickhouseBad.Addr, ":", config.ClickhouseBad.Port, "?database=", config.ClickhouseBad.DbName, "&read_timeout=10&write_timeout=20")
	PrintConfig()
}

func main() {
	ticker := time.NewTicker(1 * time.Second)
	go ParseWithRedis(ticker.C)
	addr, err := determineListenAddress()
	http.HandleFunc("/gateway/statistics/create", httpServer)
	go func() {
		profilingAddress := ":8033"
		fmt.Printf("Running profiling tools on %v\n", profilingAddress)
		if err := goprof.ListenAndServe(profilingAddress); err != nil {
			panic(err)
		}
	}()
	http.Handle("/metrics", promhttp.Handler())
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
			err := dbRedisStat.Del(post...).Err()
			if err != nil {
				log.Println(err)
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
			badJson.json = val
			badJsonArr = append(badJsonArr, badJson)
			log.Println(err)
			continue
		}
		q, err := jsonParser(jsonRaw)
		if err != nil {
			log.Println("jsonParser", err)
			continue
		}
		err = dbRedisIp.Set(strconv.Itoa(jsonRaw.Point), ip, 0).Err()
		if err != nil {
			log.Println(err)
		}
		goodJson = append(goodJson, q...)

	}

	if len(badJsonArr) != 0 {
		err = sendToBadClick(badJsonArr)
		if err != nil {
			log.Println("Send to badJson: ", err)
		}
	}
	if len(goodJson) != 0 {
		err = sendToClick(goodJson)
		if err != nil {
			log.Println("Send to stat: ", err)
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
					return query, err
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

func validateTypeJson(jsonText interface{}) (lib.Json, error) {
	var rawJson lib.Json
	switch jsonText.(type) {
	case string:
		err := json.Unmarshal([]byte(jsonText.(string)), &rawJson)
		if err != nil {
			log.Println("err json")
			return rawJson, err
		}
	default:
		return rawJson, fmt.Errorf("unknow error")
	}
	if rawJson.Point == 0 {
		return rawJson, fmt.Errorf("WARNING: point == 0")
	}
	if len(rawJson.Statistics) == 0 {
		return rawJson, fmt.Errorf("Corutpted json")
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
		fmt.Print(config.Port)
		return config.Port, nil
	} else {
		return ":" + port, nil
	}
}

func httpServer(w http.ResponseWriter, r *http.Request) {
	ip := getRealAddr(r)
	id := uuid.NewV4()
	data := r.PostFormValue("data")
	err := dbRedisStat.Set(fmt.Sprint(id, "_ip:", ip), data, 0).Err()
	if err != nil {
		log.Println("Redis SET http", err)
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
