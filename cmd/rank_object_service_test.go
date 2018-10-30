package main

import (
	"testing"

	"github.com/mimir-news/news-ranker/pkg/repository"
	"github.com/mimir-news/pkg/mq/mqtest"
	"github.com/mimir-news/pkg/schema/news"
	"github.com/stretchr/testify/assert"
)

func TestParseRankObject(t *testing.T) {
	assert := assert.New(t)

	ro := getTestRankObject()
	rankObject, err := parseRankObject(mqtest.NewMessage(ro, false, false))

	// Tests
	assert.Nil(err)
	assert.Equal(ro.String(), rankObject.String())

	rankObject, err = parseRankObject(mqtest.NewMessage("will fail to parse", false, false))
	assert.NotNil(err)
	emptyRO := news.RankObject{}
	assert.Equal(emptyRO.String(), rankObject.String())
}

func TestNewScrapeTarget(t *testing.T) {
	assert := assert.New(t)

	ro := getTestRankObject()
	article := news.Article{
		ID:    "a-0",
		URL:   "http://url.0",
		Title: "title",
		Body:  "body",
	}

	scrapeTarget := newScrapeTarget(article, ro)
	assert.Equal(article.ID, scrapeTarget.ArticleID)
	assert.Equal(article.URL, scrapeTarget.URL)
	assert.Equal("", scrapeTarget.Title)
	assert.Equal("", scrapeTarget.Body)
	assert.Equal(ro.Referer.String(), scrapeTarget.Referer.String())
	assert.Equal(len(ro.Subjects), len(scrapeTarget.Subjects))
}

func TestHandleRankObjectMessage_NewArticle(t *testing.T) {
	assert := assert.New(t)

	ro := getTestRankObject()
	articleURL := ro.URLs[0]
	message := mqtest.NewMessage(ro, false, false)

	articleRepo := &mockArticleRepo{
		findByURLErr: repository.ErrNoSuchArticle,
	}

	mockEnv := &env{
		config: Config{
			MQ: MQConfig{
				Exchange:    "mq-exchange",
				ScrapeQueue: "scrape-queue",
			},
		},
		mqClient:    mqtest.NewSuccessMockClient(nil),
		articleRepo: articleRepo,
	}

	err := mockEnv.handleRankObjectMessage(message)
	assert.Nil(err)
	assert.Equal(articleURL, articleRepo.findByURLArg)

	// Checks that no attempt was made to update an article.
	assert.Equal("", articleRepo.findArticleReferersArg)
	assert.Equal("", articleRepo.findArticleSubjectsArg)

	mockEnv.mqClient = mqtest.NewMockClient(nil, false, true, false)

	err = mockEnv.handleRankObjectMessage(message)
	assert.Nil(err)
	assert.Equal(articleURL, articleRepo.findByURLArg)

	// Checks that no attempt was made to update an article.
	assert.Equal("", articleRepo.findArticleReferersArg)
	assert.Equal("", articleRepo.findArticleSubjectsArg)

	failingRepo := &mockArticleRepo{
		findByURLErr: mockError,
	}
	mockEnv.articleRepo = failingRepo
	mockEnv.mqClient = mqtest.NewSuccessMockClient(nil)

	err = mockEnv.handleRankObjectMessage(message)
	assert.Nil(err)
	assert.Equal(articleURL, articleRepo.findByURLArg)

	// Checks that no attempt was made to update an article.
	assert.Equal("", articleRepo.findArticleReferersArg)
	assert.Equal("", articleRepo.findArticleSubjectsArg)
}

func getTestRankObject() news.RankObject {
	return news.RankObject{
		URLs: []string{
			"http://url.0",
		},
		Subjects: []news.Subject{
			news.Subject{
				Symbol: "S0",
				Name:   "subject-0",
			},
			news.Subject{
				Symbol: "S1",
				Name:   "subject-1",
			},
		},
		Referer: news.Referer{
			ExternalID:    "e-id-0",
			FollowerCount: 1000,
		},
		Language: "en",
	}
}
