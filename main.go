package main

import (
	"CreateStatistics/models"
	"CreateStatistics/parser"
	"CreateStatistics/system"
	"CreateStatistics/web"
	"flag"
	"fmt"
	"github.com/lazada/goprof"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/satori/go.uuid"
	"log"
	"net/http"
	"os"
	"time"
	"CreateStatistics/lib"
)


var (
	sendLog  = flag.Bool("sendlog", true, "send log")
	printVer = flag.Bool("v", false, "version")
)

var (
	config lib.Config
)

var (
	buildstamp string
	githash    string
	version	string
)

func init() {
	flag.Parse()
	if *printVer {
		fmt.Println("Build time: ", buildstamp)
		fmt.Println("Git hash:   ", githash)
		fmt.Println("Version:    ", version)
		os.Exit(1)
		return
	}	 
	lib.ConfigClickhouseGood, config = system.Configure()

	if !*sendLog {
		log.Println("Логи не будут отправляться")
		lib.DBClickhouseBad = models.NewClick(lib.ConfigClickhouseBad)
	}
	lib.DBClickhouseBad = models.NewPostSql("user=uid0001 dbname=stat password=music888")
	lib.DBClickhouseGood = models.NewClick(lib.ConfigClickhouseGood)
	lib.DBRedisStat = models.NewRedis(config.RedisStat.Addr, config.RedisStat.Password)
	lib.DBRedisIp = models.NewRedis(config.RedisIP.Addr, config.RedisIP.Password)
}

func main() {
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
	defer lib.DBClickhouseBad.Close()
	defer lib.DBClickhouseGood.Close()
	defer lib.DBRedisIp.Close()
	defer lib.DBRedisStat.Close()
}

func parseWithRedis(ticker <-chan time.Time) {
	for {
		select {
		case <-ticker:
			go parser.PrepareJson(*sendLog, lib.DBRedisStat, lib.DBRedisIp, lib.DBClickhouseBad, lib.DBClickhouseGood)
		}
	}
}

func httpServer(w http.ResponseWriter, r *http.Request) {
	ip := web.GetRealAddr(r)
	id := uuid.NewV4()
	uagent := r.UserAgent()
	data := r.PostFormValue("data")
	err := lib.DBRedisStat.Set(fmt.Sprint(id, "_ip:", ip, "user_agent:", uagent), data, 0).Err()
	if err != nil {
		log.Println("Redis SET http", err)
		return
	}
	fmt.Fprint(w, `{"success":true}`)
}
