package system

import (
	"os"
	"strconv"
	"github.com/olekukonko/tablewriter"
	"io/ioutil"
	"fmt"
	"encoding/json"
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
	Port string `json:"Port"`
}

func DetermineListenAddress(portWithConfig string) (string, error) {
	port := os.Getenv("PORT")
	if port == "" {
		return portWithConfig, nil
	} else {
		return ":" + port, nil
	}
}

func Configure() (string, string, Config) {
	var config Config
	file, err := ioutil.ReadFile("config/CreateStatistics.config")
	if err != nil {
		fmt.Println(err)
	}
	err = json.Unmarshal(file, &config)
	if err != nil {
		fmt.Println("Unmarshal config", err)
	}
	configClickhouseGood := fmt.Sprint("tcp://", config.ClickhouseGood.Addr, ":", config.ClickhouseGood.Port, "?database=", config.ClickhouseGood.DbName, "&read_timeout=10&write_timeout=20")
	configClickhouseBad := fmt.Sprint("tcp://", config.ClickhouseBad.Addr, ":", config.ClickhouseBad.Port, "?database=", config.ClickhouseBad.DbName, "&read_timeout=10&write_timeout=20")
	printConfig(config)
	return configClickhouseBad, configClickhouseGood, config
}

func printConfig(config Config) {
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
