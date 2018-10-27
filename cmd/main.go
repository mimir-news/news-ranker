package main

import (
	"sync"
)

func main() {
	conf := newConfig()
	e := setupEnv(conf)
	wg := &sync.WaitGroup{}

	rankObjectHandler := e.newSubscriptionHandler(e.rankQueue(), e.handleRankObjectMessage)
	articlesHandler := e.newSubscriptionHandler(e.scrapedQueue(), e.handleScrapedArticleMessage)
	go handleSubscription(rankObjectHandler, wg)
	go handleSubscription(articlesHandler, wg)

	wg.Wait()
}

func newConfig() Config {
	return Config{
		MQ: MQConfig{
			Host:         "localhost",
			Port:         "5672",
			User:         "newsranker",
			Password:     "password",
			Exchange:     "x-news",
			ScrapeQueue:  "q-scrape-targets",
			ScrapedQueue: "q-scraped-articles",
			RankQueue:    "q-rank-objects",
		},
	}
}
