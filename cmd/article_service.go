package main

import (
	"github.com/mimir-news/pkg/mq"
	"github.com/mimir-news/pkg/schema/news"
)

func (e *env) handleScrapedArticleMessage(msg mq.Message, msgID string) error {
	logger.Infow("Incomming ScrapedArticle", "msgID", msgID)
	scrapedArticle, err := parseScrapedArticle(msg)
	if err != nil {
		logger.Errorw("Parsing ScrapedArticle failed", "msgID", msgID, "err", err)
		return err
	}

	article, err := e.updateAndStoreScrapedArticle(scrapedArticle)
	if err != nil {
		logger.Errorw("Failed to store scraped article", "msgID", msgID, "err", err)
		return nil
	}

	e.clusterArticle(article)
	logger.Infow("Success in handling ScrapedArticle", "msgID", msgID)
	return nil
}

func (e *env) updateAndStoreScrapedArticle(scrapedArticle news.ScrapedArticle) (news.Article, error) {
	referers, err := e.articleRepo.FindArticleReferers(scrapedArticle.Article.ID)
	if err != nil {
		return news.Article{}, err
	}

	mergedReferers := mergeReferers(referers, scrapedArticle.Referer)
	referenceScore := calcReferenceScore(e.config.TwitterUsers, e.config.ReferenceWeight, mergedReferers...)
	scrapedArticle.Article.ReferenceScore = referenceScore

	err = e.articleRepo.SaveScrapedArticle(scrapedArticle)
	if err != nil {
		return news.Article{}, err
	}
	return scrapedArticle.Article, nil
}

func parseScrapedArticle(msg mq.Message) (news.ScrapedArticle, error) {
	var sa news.ScrapedArticle
	err := msg.Decode(&sa)
	return sa, err
}

func mergeReferers(referers []news.Referer, newReferer news.Referer) []news.Referer {
	merged := make([]news.Referer, len(referers))
	copy(merged, referers)

	for _, referer := range referers {
		if referer.ExternalID == newReferer.ExternalID {
			return merged
		}
	}
	merged = append(merged, newReferer)
	return merged
}
