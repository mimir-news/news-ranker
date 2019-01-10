package main

import (
	"github.com/mimir-news/news-ranker/pkg/domain"
	"github.com/mimir-news/news-ranker/pkg/repository"
	"github.com/mimir-news/pkg/id"
	"github.com/mimir-news/pkg/mq"
	"github.com/mimir-news/pkg/schema/news"
)

func (e *env) handleRankObjectMessage(msg mq.Message, msgID string) error {
	logger.Infow("Incomming RankObject", "msgID", msgID)
	ro, err := parseRankObject(msg)
	if err != nil {
		logger.Errorw("Parsing RankObject failed", "msgID", msgID, "err", err)
		return err
	}

	for _, URL := range ro.URLs {
		article, err := e.articleRepo.FindByURL(URL)
		if err == repository.ErrNoSuchArticle {
			e.rankNewArticle(news.NewArticle(URL), ro)
			continue
		} else if err != nil {
			logger.Errorw("Getting article from repository failed", "msgID", msgID, "err", err)
			continue
		}
		e.rankExistingArticle(article, ro)
	}
	logger.Infow("Success in handling RankObject", "msgID", msgID)
	return nil
}

func (e *env) rankNewArticle(article news.Article, rankObject news.RankObject) {
	scrapeTarget := newScrapeTarget(article, rankObject)
	e.queueScrapeTarget(scrapeTarget)
}

func (e *env) rankExistingArticle(article news.Article, rankObject news.RankObject) {
	update, err := e.getArticleUpdate(article, rankObject)
	if err != nil {
		logger.Errorw("Getting article from repository failed", "err", err)
		return
	}

	switch update.Type {
	case domain.NewSubjectsAndReferences, domain.NewSubjects:
		e.queueScrapeTarget(update.ToScapeTarget())
	case domain.NewReferences:
		e.rankWithNewReferences(update)
	default:
		logger.Infow("Taking no action article",
			"updateType", update.Type,
			"articleId", article.ID)
	}
}

func (e *env) rankWithNewReferences(update domain.ArticleUpdate) {
	newRefScore := calcReferenceScore(e.config.TwitterUsers, e.config.ReferenceWeight, update.Referers...)
	update.Article.ReferenceScore = newRefScore

	err := e.articleRepo.Update(update.Article)
	if err != nil {
		logger.Errorw("Article update failed", "err", err)
		return
	}

	err = e.articleRepo.SaveReferer(update.NewReferer)
	if err != nil {
		logger.Errorw("Saving referer failed", "err", err)
		return
	}

	e.clusterArticle(update.Article)
}

func (e *env) getArticleUpdate(article news.Article, rankObject news.RankObject) (domain.ArticleUpdate, error) {
	subjects, err := e.articleRepo.FindArticleSubjects(article.ID)
	if err != nil {
		return domain.ArticleUpdate{}, err
	}

	referers, err := e.articleRepo.FindArticleReferers(article.ID)
	if err != nil {
		return domain.ArticleUpdate{}, err
	}

	articleUpdate := domain.CreateArticleUpdate(
		article, subjects, rankObject.Subjects, referers, rankObject.Referer)
	return articleUpdate, nil
}

func (e *env) queueScrapeTarget(scrapeTarget news.ScrapeTarget) {
	logger.Infow("Sending article for scraping", "articleId", scrapeTarget.ArticleID)
	err := e.mqClient.Send(scrapeTarget, e.exchange(), e.scrapeQueue())
	if err != nil {
		logger.Errorw("Sending scrape target failed", "articleId", scrapeTarget.ArticleID, "err", err)
	}
}

func parseRankObject(msg mq.Message) (news.RankObject, error) {
	var ro news.RankObject
	err := msg.Decode(&ro)
	return ro, err
}

func calcReferenceScore(twitterUsers, referenceWeight float64, references ...news.Referer) float64 {
	var totalReferences int64
	for _, reference := range references {
		totalReferences += reference.FollowerCount
	}
	return float64(totalReferences) * referenceWeight / twitterUsers
}

func newScrapeTarget(article news.Article, ro news.RankObject) news.ScrapeTarget {
	target := news.ScrapeTarget{
		URL:       article.URL,
		Subjects:  ro.Subjects,
		ArticleID: article.ID,
		Referer:   ro.Referer,
	}

	target.Referer.ArticleID = article.ID
	if ro.Referer.ID == "" {
		target.Referer.ID = id.New()
	}

	for i := 0; i < len(target.Subjects); i++ {
		target.Subjects[i].ArticleID = article.ID
		if target.Subjects[i].ID == "" {
			target.Subjects[i].ID = id.New()
		}
	}

	return target
}
