package main

import (
	"database/sql"
	"log"

	"github.com/mimir-news/news-ranker/pkg/repository"
	"github.com/mimir-news/pkg/dbutil"
	"github.com/mimir-news/pkg/mq"
)

type env struct {
	config      config
	mqClient    mq.Client
	articleRepo repository.ArticleRepo
	clusterRepo repository.ClusterRepo
	db          *sql.DB
}

func setupEnv(conf config) *env {
	mqClient, err := mq.NewClient(conf.MQConfig())
	if err != nil {
		log.Fatal(err)
	}

	db, err := conf.DB.ConnectPostgres()
	if err != nil {
		log.Fatal(err)
	}
	runMigrations(db)

	articleRepo := repository.NewArticleRepo(db)
	clusterRepo := repository.NewClusterRepo(db)

	return &env{
		config:      conf,
		mqClient:    mqClient,
		articleRepo: articleRepo,
		clusterRepo: clusterRepo,
		db:          db,
	}
}

func runMigrations(db *sql.DB) {
	err := dbutil.Migrate("./migrations", "postgres", db)
	if err != nil {
		log.Fatal(err)
	}
}

func (e *env) close() {
	err := e.mqClient.Close()
	if err != nil {
		log.Println(err)
	}

	err = e.db.Close()
	if err != nil {
		log.Println(err)
	}
}

func (e *env) newSubscriptionHandler(queue string, fn handlerFunc) handler {
	return newHandler(queue, e.mqClient, fn)
}

func (e *env) exchange() string {
	return e.config.MQ.Exchange
}

func (e *env) rankQueue() string {
	return e.config.MQ.RankQueue
}

func (e *env) scrapeQueue() string {
	return e.config.MQ.ScrapeQueue
}

func (e *env) scrapedQueue() string {
	return e.config.MQ.ScrapedQueue
}
