package main

import (
	"log"

	"github.com/mimir-news/news-ranker/pkg/domain"
	"github.com/mimir-news/news-ranker/pkg/repository"
	"github.com/mimir-news/pkg/id"
	"github.com/mimir-news/pkg/mq"
	"github.com/mimir-news/pkg/schema/news"
)

func (e *env) handleRankObjectMessage(msg mq.Message) error {
	rankObject, err := parseRankObject(msg)
	if err != nil {
		return err
	}

	for _, URL := range rankObject.URLs {
		article, err := e.articleRepo.FindByURL(URL)
		if err == repository.ErrNoSuchArticle {
			e.rankNewArticle(news.NewArticle(URL), rankObject)
			continue
		} else if err != nil {
			log.Println(err)
			continue
		}
		e.rankExistingArticle(article, rankObject)
	}
	return nil
}

func (e *env) rankNewArticle(article news.Article, rankObject news.RankObject) {
	scrapeTarget := newScrapeTarget(article, rankObject)
	e.queueScrapeTarget(scrapeTarget)
}

func (e *env) rankExistingArticle(article news.Article, rankObject news.RankObject) {
	update, err := e.getArticleUpdate(article, rankObject)
	if err != nil {
		log.Println(err)
		return
	}

	switch update.Type {
	case domain.NEW_SUBJECTS_AND_REFERENCES, domain.NEW_SUBJECTS:
		e.queueScrapeTarget(update.ToScapeTarget())
	case domain.NEW_REFERENCES:
		e.rankWithNewReferences(update)
	default:
		log.Printf("Taking no action on update type: %d for article: %s\n",
			update.Type, article.ID)
	}
}

func (e *env) rankWithNewReferences(update domain.ArticleUpdate) {
	newRefScore := calcReferenceScore(e.config.TwitterUsers, e.config.ReferenceWeight, update.Referers...)
	update.Article.ReferenceScore = newRefScore

	err := e.articleRepo.Update(update.Article)
	if err != nil {
		log.Println(err)
		return
	}

	err = e.articleRepo.SaveReferer(update.NewReferer)
	if err != nil {
		log.Println(err)
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
	err := e.mqClient.Send(scrapeTarget, e.exchange(), e.scrapeQueue())
	if err != nil {
		log.Println(err)
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
