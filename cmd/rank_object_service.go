package main

import (
	"log"

	"github.com/mimir-news/news-ranker/pkg/domain"
	"github.com/mimir-news/news-ranker/pkg/repository"
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
	article.ReferenceScore = calcReferenceScore(e.config.TwitterUsers, rankObject.Referer)
	scrapeTarget := newScrapeTarget(article, rankObject)

	err := e.mqClient.Send(scrapeTarget, e.exchange(), e.scrapeQueue())
	if err != nil {
		log.Println(err)
	}
}

func (e *env) rankExistingArticle(article news.Article, rankObject news.RankObject) {
	update, err := e.getArticleUpdate(article, rankObject)
	if err != nil {
		log.Println(err)
		return
	}

	switch update.Type {
	case domain.NEW_SUBJECTS_AND_REFERENCES:
	case domain.NEW_SUBJECTS:
		e.rankWithNewSubjects(update)
	case domain.NEW_REFERENCES:
		e.rankWithNewReferences(update)
	default:
		log.Printf("Taking no action on update type: %d for article: %s\n",
			update.Type, article.ID)
	}
}

func (e *env) rankWithNewSubjects(update domain.ArticleUpdate) {
	scrapeTarget := update.ToScapeTarget()

	err := e.mqClient.Send(scrapeTarget, e.exchange(), e.scrapeQueue())
	if err != nil {
		log.Println(err)
	}
}

func (e *env) rankWithNewReferences(update domain.ArticleUpdate) {
	newRefScore := calcReferenceScore(e.config.TwitterUsers, update.Referers...)
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

func parseRankObject(msg mq.Message) (news.RankObject, error) {
	var ro news.RankObject
	err := msg.Decode(&ro)
	return ro, err
}

func calcReferenceScore(twitterUsers int64, references ...news.Referer) float64 {
	var totalReferences int64
	for _, reference := range references {
		totalReferences += reference.FollowerCount
	}
	return float64(totalReferences) / float64(twitterUsers)
}

func newScrapeTarget(article news.Article, ro news.RankObject) news.ScrapeTarget {
	return news.ScrapeTarget{
		URL:            article.URL,
		Subjects:       ro.Subjects,
		ReferenceScore: article.ReferenceScore,
		ArticleID:      article.ID,
		Referer:        ro.Referer,
	}
}
