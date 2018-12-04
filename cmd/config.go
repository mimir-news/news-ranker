package main

import (
	"log"
	"os"
	"strconv"

	"github.com/mimir-news/pkg/dbutil"
	"github.com/mimir-news/pkg/mq"
)

// Service metadata.
const (
	ServiceName = "news-ranker"
)

type config struct {
	MQ               mqConfig
	DB               dbutil.Config
	TwitterUsers     int64
	HearbeatFile     string
	HearbeatInterval int
}

type mqConfig struct {
	Host         string
	Port         string
	User         string
	Password     string
	Exchange     string
	ScrapeQueue  string
	ScrapedQueue string
	RankQueue    string
}

func mustGetMQConfig() mqConfig {
	return mqConfig{
		Host:         mustGetenv("MQ_HOST"),
		Port:         getenv("MQ_PORT", "5672"),
		User:         mustGetenv("MQ_USER"),
		Password:     mustGetenv("MQ_PASSWORD"),
		Exchange:     mustGetenv("MQ_EXCHANGE"),
		ScrapeQueue:  mustGetenv("MQ_SCRAPE_QUEUE"),
		ScrapedQueue: mustGetenv("MQ_SCRAPED_QUEUE"),
		RankQueue:    mustGetenv("MQ_RANK_QUEUE"),
	}
}

func getConfig() config {
	twitterUsersStr := getenv("TWITTER_USERS", "320000000")
	twitterUsers, err := strconv.ParseInt(twitterUsersStr, 10, 64)
	if err != nil {
		log.Fatalln("TWITTER_USERS parsing failed", err)
	}

	interval, err := strconv.Atoi(getenv("HEARTBEAT_INTERVAL", "20"))
	if err != nil {
		log.Fatalln("HEARTBEAT_INTERVAL parsing failed", err)
	}

	return config{
		MQ:               mustGetMQConfig(),
		DB:               dbutil.MustGetConfig("DB"),
		TwitterUsers:     twitterUsers,
		HearbeatFile:     mustGetenv("HEARTBEAT_FILE"),
		HearbeatInterval: interval,
	}
}

func (c config) MQConfig() mq.Config {
	return mq.NewConfig(c.MQ.Host, c.MQ.Port, c.MQ.User, c.MQ.Password)
}

func mustGetenv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("No value for key: %s\n", key)
	}

	return val
}

func getenv(key, defaultVal string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}

	return val
}
