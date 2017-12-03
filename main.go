package main

import (
	"CreateStatistics/models"
	"CreateStatistics/parser"
	"CreateStatistics/system"
	"CreateStatistics/web"
	"database/sql"
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/lazada/goprof"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/satori/go.uuid"
	"log"
	"net/http"
	"time"
)

var (
	dbClickhouseBad      *sql.DB
	dbClickhouseGood     *sql.DB
	configClickhouseBad  string
	configClickhouseGood string
	dbRedisStat          *redis.Client
	dbRedisIp            *redis.Client
	config               system.Config
)

var (
	sendLog = flag.Bool("sendlog", true, "Отправлять статистику?")
	printVer = flag.Bool("v", false, "Версия")
	)

var (
	buildstamp string
	githash string
)

func init() {
	configClickhouseBad, configClickhouseGood, config = system.Configure()
	flag.Parse()
	if !*sendLog {
		log.Println("Логи не будут отправляться")
		dbClickhouseBad = models.NewClick(configClickhouseBad)
	}
	dbClickhouseBad = models.NewClick(configClickhouseBad)
	dbClickhouseGood = models.NewClick(configClickhouseGood)
	dbRedisStat = models.NewRedis(config.RedisStat.Addr, config.RedisStat.Password)
	dbRedisIp = models.NewRedis(config.RedisIP.Addr, config.RedisIP.Password)
}

func main() {
	if *printVer {
		fmt.Println(buildstamp)
		fmt.Println(githash)
		return
	}
	ticker := time.NewTicker(3 * time.Second)
	go parseWithRedis(ticker.C)
	addr, err := system.DetermineListenAddress(config.Port)
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

func parseWithRedis(ticker <-chan time.Time) {
	//	postArr := make(chan []string)
	for {
		select {
		case <-ticker:
			go parser.PrepareJson(*sendLog, dbRedisStat, dbRedisIp, dbClickhouseBad, dbClickhouseGood)
		}
	}
}

func httpServer(w http.ResponseWriter, r *http.Request) {
	ip := web.GetRealAddr(r)
	id := uuid.NewV4()
	uagent := r.UserAgent()
	data := r.PostFormValue("data")
	err := dbRedisStat.Set(fmt.Sprint(id, "_ip:", ip, "user_agent:", uagent), data, 0).Err()
	if err != nil {
		log.Println("Redis SET http", err)
		return
	}
	fmt.Fprint(w, `{"success":true}`)
}
