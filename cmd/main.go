package main

import (
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func main() {
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
	log.Println("Started", ServiceName)
	wg.Wait()
}
