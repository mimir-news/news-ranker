package main

import (
	"testing"
	"time"

	"github.com/mimir-news/pkg/mq/mqtest"
	"github.com/mimir-news/pkg/schema/news"
	"github.com/stretchr/testify/assert"
)

func TestHandleScrapedArticleMessage_Success(t *testing.T) {
	assert := assert.New(t)

	scrapedArticle := getTestScrapedArticle()

	message := mqtest.NewMessage(scrapedArticle, false, false)

	oldReferers := []news.Referer{
		news.Referer{
			ID:            "r-0",
			ExternalID:    "e-id-0",
			FollowerCount: 1000,
			ArticleID:     "a-0",
		},
		news.Referer{
			ID:            "r-1",
			ExternalID:    "e-id-1",
			FollowerCount: 1000,
			ArticleID:     "a-0",
		},
	}

	articleRepo := &mockArticleRepo{
		articleReferers:        oldReferers,
		findArticleReferersErr: nil,
		saveScrapedArticleErr:  nil,
		articleSubjects:        nil, // Set up to prevent clusterering which is not in scope for the test.
		findArticleSubjectsErr: errMock,
	}
	mockEnv := &env{
		config:      config{TwitterUsers: 6000},
		articleRepo: articleRepo,
	}

	err := mockEnv.handleScrapedArticleMessage(message)
	assert.Nil(err)

	assert.Equal(scrapedArticle.Article.ID, articleRepo.findArticleReferersArg)
	assertScore(0.5, articleRepo.saveScrapedArticleArg.Article.ReferenceScore, t)
}

func TestHandleScrapedArticleMessage_FailedParse(t *testing.T) {
	message := mqtest.NewMessage([]byte("will not parse"), false, false)
	mockEnv := &env{}
	err := mockEnv.handleScrapedArticleMessage(message)
	assert.NotNil(t, err)
}

func TestHandleScrapedArticleMessage_FailedDBInteractions(t *testing.T) {
	assert := assert.New(t)

	scrapedArticle := getTestScrapedArticle()

	message := mqtest.NewMessage(scrapedArticle, false, false)

	articleRepoNoReferers := &mockArticleRepo{
		articleReferers:        nil,
		findArticleReferersErr: errMock,
	}

	mockEnv := &env{
		config:      config{TwitterUsers: 2000},
		articleRepo: articleRepoNoReferers,
	}

	err := mockEnv.handleScrapedArticleMessage(message)
	assert.Nil(err)
	assert.Equal(scrapedArticle.Article.ID, articleRepoNoReferers.findArticleReferersArg)
	assert.Equal("", articleRepoNoReferers.saveScrapedArticleArg.Article.ID)

	oldReferers := []news.Referer{
		news.Referer{
			ID:            "r-new",
			ExternalID:    "e-id-new",
			FollowerCount: 1000,
			ArticleID:     "a-0",
		},
	}

	articleRepoFailedSave := &mockArticleRepo{
		articleReferers:        oldReferers,
		findArticleReferersErr: nil,
		saveScrapedArticleErr:  errMock,
	}

	mockEnv.articleRepo = articleRepoFailedSave

	err = mockEnv.handleScrapedArticleMessage(message)
	assert.Nil(err)
	assert.Equal(scrapedArticle.Article.ID, articleRepoFailedSave.findArticleReferersArg)
	assert.Equal(scrapedArticle.Article.ID, articleRepoFailedSave.saveScrapedArticleArg.Article.ID)
	assertScore(0.5, articleRepoFailedSave.saveScrapedArticleArg.Article.ReferenceScore, t)
}

func getTestScrapedArticle() news.ScrapedArticle {
	return news.ScrapedArticle{
		Article: news.Article{
			ID:    "a-0",
			URL:   "http://url.com",
			Title: "title-0",
			Body:  "Body text",
			Keywords: []string{
				"w0",
				"w1",
			},
			ArticleDate: time.Now(),
		},
		Subjects: []news.Subject{
			news.Subject{
				ID:        "s-0",
				Symbol:    "S0",
				Name:      "subject-0",
				Score:     0.1,
				ArticleID: "a-0",
			},
			news.Subject{
				ID:        "s-1",
				Symbol:    "S1",
				Name:      "subject-1",
				Score:     0.2,
				ArticleID: "a-0",
			},
		},
		Referer: news.Referer{
			ID:            "r-new",
			ExternalID:    "e-id-new",
			FollowerCount: 1000,
			ArticleID:     "a-0",
		},
	}
}

type mockArticleRepo struct {
	findByURLArg     string
	findByURLArticle news.Article
	findByURLErr     error

	findArticleSubjectsArg string
	articleSubjects        []news.Subject
	findArticleSubjectsErr error

	findArticleReferersArg string
	articleReferers        []news.Referer
	findArticleReferersErr error

	updateArg news.Article
	updateErr error

	saveRefererArg news.Referer
	saveRefererErr error

	saveScrapedArticleArg news.ScrapedArticle
	saveScrapedArticleErr error
}

func (r *mockArticleRepo) FindByURL(url string) (news.Article, error) {
	r.findByURLArg = url
	return r.findByURLArticle, r.findByURLErr
}

func (r *mockArticleRepo) FindArticleSubjects(articleID string) ([]news.Subject, error) {
	r.findArticleSubjectsArg = articleID
	return r.articleSubjects, r.findArticleSubjectsErr
}

func (r *mockArticleRepo) FindArticleReferers(articleID string) ([]news.Referer, error) {
	r.findArticleReferersArg = articleID
	return r.articleReferers, r.findArticleReferersErr
}

func (r *mockArticleRepo) Update(article news.Article) error {
	r.updateArg = article
	return r.updateErr
}

func (r *mockArticleRepo) SaveReferer(referer news.Referer) error {
	r.saveRefererArg = referer
	return r.saveRefererErr
}

func (r *mockArticleRepo) SaveScrapedArticle(scrapedArticle news.ScrapedArticle) error {
	r.saveScrapedArticleArg = scrapedArticle
	return r.saveScrapedArticleErr
}
