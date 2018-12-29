package main

import (
	"sync"
)

func main() {
	conf := getConfig()
	e := setupEnv(conf)
	wg := &sync.WaitGroup{}

	rankObjectHandler := e.newSubscriptionHandler(e.rankQueue(), e.handleRankObjectMessage)
	articlesHandler := e.newSubscriptionHandler(e.scrapedQueue(), e.handleScrapedArticleMessage)
	go e.healthCheck()
	go handleSubscription(rankObjectHandler, wg)
	go handleSubscription(articlesHandler, wg)

	wg.Wait()
}
