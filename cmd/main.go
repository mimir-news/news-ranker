package main

import (
	"sync"

	"github.com/CzarSimon/go-file-heartbeat/heartbeat"
)

func main() {
	conf := getConfig()
	e := setupEnv(conf)
	wg := &sync.WaitGroup{}

	rankObjectHandler := e.newSubscriptionHandler(e.rankQueue(), e.handleRankObjectMessage)
	articlesHandler := e.newSubscriptionHandler(e.scrapedQueue(), e.handleScrapedArticleMessage)
	go emitHeartbeats(conf)
	go handleSubscription(rankObjectHandler, wg)
	go handleSubscription(articlesHandler, wg)

	wg.Wait()
}

func emitHeartbeats(conf config) {
	heartbeat.RunFileHeartbeat(conf.HearbeatFile, conf.HearbeatInterval)
}
