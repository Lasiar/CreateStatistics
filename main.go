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
)

var (
	dbClickhouseBad      *sql.DB
	dbClickhouseGood     *sql.DB
	configClickhouseBad  string
	configClockhouseGood string
	dbRedisStat          *redis.Client
	dbRedisIp            *redis.Client
	configRedisStat      string
	configRedisIp        string
	config               Config
)

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

func init() {
	var err error
	Configure()

	dbClickhouseBad, err = sql.Open("clickhouse", "tcp://stat.krasrm.com:9000?database=BadJson&read_timeout=10&write_timeout=20")
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
	dbClickhouseGood, err = sql.Open("clickhouse", "tcp://stat.krasrm.com:9000?database=stat&read_timeout=10&write_timeout=20")
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
		Addr:     "ec2-34-236-65-51.compute-1.amazonaws.com:50399",
		Password: "pb23ec6d25b0c81ca69a2cf43875c3cb2135b3289071f0216d501c6d0f3620001", // no password set
		DB:       0,                                                                   // use default DB
	})
	_, err = dbRedisStat.Ping().Result()
	if err != nil {
		log.Println(err)
	}
	dbRedisIp = redis.NewClient(&redis.Options{
		Addr:     "ec2-34-236-132-20.compute-1.amazonaws.com:16779",
		Password: "paeea1b403c6b65699b6df648f97eae02af0759026316d0832693297736661ee1", // no password set
		DB:       0,                                                                   // use default DB
	})
	_, err = dbRedisStat.Ping().Result()
	if err != nil {
		log.Println(err)
	}
}

func main() {
	addr, err := determineListenAddress()
	http.HandleFunc("/gateway/statistics/create", httpServer)
	err = http.ListenAndServe(addr, nil) // set listen port
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
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

func Configure()  {
	file, err := ioutil.ReadFile("config/CreateStatistics.config")
	if err != nil {
		fmt.Println(err)
	}
	err = json.Unmarshal(file, &config)
	if err != nil {
		fmt.Println("Unmarshal config", err)
	}
	PrintConfig()
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
