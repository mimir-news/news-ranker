package main

import (
	"log"

	envConf "github.com/caarlos0/env"
	"github.com/mimir-news/pkg/dbutil"
	"github.com/mimir-news/pkg/mq"
)

const (
	SERVICE_NAME = "news-ranker"
)

type config struct {
	MQ               mqConfig      `env:"MQ"`
	DB               dbutil.Config `env:"DB"`
	TwitterUsers     int64         `env:"TWITTER_USERS" envDefault:"320000000"`
	HearbeatFile     string        `env:"HEARTBEAT_FILE"`
	HearbeatInterval int           `env:"HEARTBEAT_INTERVAL"`
}

type mqConfig struct {
	Host         string `env:"HOST"`
	Port         string `env:"PORT"`
	User         string `env:"USER"`
	Password     string `env:"PASSWORD"`
	Exchange     string `env:"EXCHANGE"`
	ScrapeQueue  string `env:"SCRAPE_QUEUE"`
	ScrapedQueue string `env:"SCRAPED_QUEUE"`
	RankQueue    string `env:"RANK_QUEUE"`
}

func getConfig() config {
	var conf config
	err := envConf.Parse(&conf)
	if err != nil {
		log.Fatal(err)
	}
	return conf
}

func (c config) MQConfig() mq.Config {
	return mq.NewConfig(c.MQ.Host, c.MQ.Port, c.MQ.User, c.MQ.Password)
}
