package main

import (
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

var logger *zap.SugaredLogger

func init() {
	l, err := zap.NewProduction()
	if err != nil {
		log.Fatal("main.init zap.Logger init failed.", err)
	}

	logger = l.Sugar()
}

func main() {
	defer logger.Sync()
	conf := getConfig()
	e := setupEnv(conf)
	defer e.close()
	wg := &sync.WaitGroup{}

	rankObjectHandler := e.newSubscriptionHandler(e.rankQueue(), e.handleRankObjectMessage)
	articlesHandler := e.newSubscriptionHandler(e.scrapedQueue(), e.handleScrapedArticleMessage)
	go e.healthCheck()
	go handleSubscription(rankObjectHandler, wg)
	go handleSubscription(articlesHandler, wg)

	time.Sleep(initalWaitingTime)
	logger.Infow("Application started", "name", ServiceName) // log.Println("Started", ServiceName)
	wg.Wait()
}
