package main

import (
	"errors"
	"testing"
	"time"

	"github.com/mimir-news/news-ranker/pkg/domain"
	"github.com/mimir-news/news-ranker/pkg/repository"
	"github.com/mimir-news/pkg/id"
	"github.com/mimir-news/pkg/mq"
	"github.com/mimir-news/pkg/schema/news"
	"github.com/stretchr/testify/assert"
)

var mockError = errors.New("mock error")
var emptyCluster = domain.ArticleCluster{}

func TestCreateNewCluster(t *testing.T) {
	assert := assert.New(t)

	articleDate, err := time.Parse("2006-01-02", "2018-10-25")
	assert.Nil(err)

	article := news.Article{
		ID:             "a-0",
		URL:            "http://url.com",
		Title:          "t-0",
		ReferenceScore: 0.5,
		ArticleDate:    articleDate,
	}
	subject := news.Subject{
		Symbol:    "smbl",
		Score:     0.3,
		ArticleID: "a-0",
	}
	clusterHash := domain.CalcClusterHash(article.Title, subject.Symbol, articleDate)

	clusterRepo := &mockClusterRepo{
		findByHashCluster: emptyCluster,
		findByHashErr:     repository.ErrNoSuchCluster,
		saveReturn:        nil,
	}
	mockEnv := newMockEnv(nil, clusterRepo, nil)

	mockEnv.clusterArticleWithSubject(article, subject)

	assert.Equal(clusterHash, clusterRepo.findByHashArg)
	cluster := clusterRepo.saveArg

	assertArticleCluster(clusterHash, article, subject, cluster, t)
	assert.Equal(0.8, cluster.Score)
	assert.Equal(1, len(cluster.Members))

	member := cluster.Members[0]
	assert.Equal(article.ID, member.ArticleID)
	assert.Equal(subject.Score, member.SubjectScore)
	assert.Equal(article.ReferenceScore, member.ReferenceScore)

	clusterRepo = &mockClusterRepo{
		findByHashCluster: emptyCluster,
		findByHashErr:     repository.ErrNoSuchCluster,
		saveReturn:        mockError,
	}
	mockEnv = newMockEnv(nil, clusterRepo, nil)

	mockEnv.clusterArticleWithSubject(article, subject)

	assert.Equal(clusterHash, clusterRepo.findByHashArg)
	cluster = clusterRepo.saveArg
	assert.Equal(clusterHash, cluster.Hash)

	clusterRepo = &mockClusterRepo{
		findByHashCluster: emptyCluster,
		findByHashErr:     mockError,
	}
	mockEnv = newMockEnv(nil, clusterRepo, nil)

	mockEnv.clusterArticleWithSubject(article, subject)

	assert.Equal(clusterHash, clusterRepo.findByHashArg)
	cluster = clusterRepo.saveArg
	assert.Equal("", cluster.Hash)
}

func TestUpdateArticleCluster(t *testing.T) {
	assert := assert.New(t)

	articleDate, err := time.Parse("2006-01-02", "2018-10-25")
	assert.Nil(err)
	symbol := "symbol-0"
	articleTitle := "title-0"
	clusterHash := domain.CalcClusterHash(articleTitle, symbol, articleDate)

	newArticle := news.Article{
		ID:             "a-new",
		URL:            "http://url.com",
		Title:          articleTitle,
		ReferenceScore: 0.5,
		ArticleDate:    articleDate,
	}

	subject := news.Subject{
		Symbol:    symbol,
		Score:     0.3,
		ArticleID: "a-new",
	}

	oldMembers := []domain.ClusterMember{
		*domain.NewClusterMember(clusterHash, "a-0", 0.3, 0.1),
		*domain.NewClusterMember(clusterHash, "a-1", 0.4, 0.2),
	}

	existingCluster := *domain.NewArticleCluster(newArticle.Title, symbol, articleDate, "a-1", 0.9, oldMembers)

	clusterRepo := &mockClusterRepo{
		findByHashCluster: existingCluster,
		findByHashErr:     nil,
		updateReturn:      nil,
	}
	mockEnv := newMockEnv(nil, clusterRepo, nil)
	mockEnv.clusterArticleWithSubject(newArticle, subject)

	assert.Equal(clusterHash, clusterRepo.findByHashArg)
	cluster := clusterRepo.updateArg
	assertArticleCluster(clusterHash, newArticle, subject, cluster, t)
	assertScore(1.5, cluster.Score, t)
	assert.Equal(3, len(cluster.Members))

	for i, score := range []float64{0.4, 0.6, 0.8} {
		member := cluster.Members[i]
		assertScore(score, member.Score(), t)
	}

	clusterRepo = &mockClusterRepo{
		findByHashCluster: existingCluster,
		findByHashErr:     nil,
		updateReturn:      mockError,
	}
	mockEnv = newMockEnv(nil, clusterRepo, nil)
	mockEnv.clusterArticleWithSubject(newArticle, subject)

	assert.Equal(clusterHash, clusterRepo.findByHashArg)
	cluster = clusterRepo.updateArg
	assertArticleCluster(clusterHash, newArticle, subject, cluster, t)
	assertScore(1.5, cluster.Score, t)
	assert.Equal(3, len(cluster.Members))
}

func TestClusterArticle(t *testing.T) {
	// Test setup
	assert := assert.New(t)

	articleDate, err := time.Parse("2006-01-02", "2018-10-25")
	assert.Nil(err)
	articleTitle := "title-0"

	article := news.Article{
		ID:             "a-new",
		URL:            "http://url.com",
		Title:          articleTitle,
		ReferenceScore: 0.5,
		ArticleDate:    articleDate,
	}

	// Mocking
	articleRepo := &mockArticleRepo{
		articleSubjects:        nil,
		findArticleSubjectsErr: mockError,
	}

	mockEnv := newMockEnv(articleRepo, nil, nil)

	// Method call
	mockEnv.clusterArticle(article)

	// Tests
	assert.Equal(article.ID, articleRepo.findArticleSubjectsArg)

	// Test setup
	subjects := []news.Subject{
		news.Subject{
			ID:        id.New(),
			Symbol:    "symbol-0",
			Name:      "First symbol",
			Score:     0.3,
			ArticleID: article.ID,
		},
		news.Subject{
			ID:        id.New(),
			Symbol:    "symbol-1",
			Name:      "Second symbol",
			Score:     0.4,
			ArticleID: article.ID,
		},
	}

	// Mocking
	articleRepo = &mockArticleRepo{
		articleSubjects:        subjects,
		findArticleSubjectsErr: nil,
	}
	clusterRepo := &mockClusterRepo{
		findByHashCluster: emptyCluster,
		findByHashErr:     mockError,
	}
	mockEnv = newMockEnv(articleRepo, clusterRepo, nil)

	// Method call
	mockEnv.clusterArticle(article)

	// Tests
	assert.Equal(article.ID, articleRepo.findArticleSubjectsArg)
	expectedHash := domain.CalcClusterHash(article.Title, subjects[1].Symbol, articleDate)
	assert.Equal(expectedHash, clusterRepo.findByHashArg)
}

func assertScore(expected, actual float64, t *testing.T) {
	expectedInt := int(expected * 10)
	actualInt := int(actual * 10)
	assert.Equal(t, expectedInt, actualInt)
}

func assertArticleCluster(eHash string, article news.Article, subject news.Subject,
	cluster domain.ArticleCluster, t *testing.T) {

	assert := assert.New(t)
	assert.Equal(eHash, cluster.Hash)
	assert.Equal(article.ID, cluster.LeadArticleID)
	assert.Equal(article.Title, cluster.Title)
	assert.Equal(subject.Symbol, cluster.Symbol)
	assert.Equal(article.ArticleDate, cluster.ArticleDate)

	for _, member := range cluster.Members {
		assert.NotEqual("", member.ID)
	}
}

func newMockEnv(
	articleRepo repository.ArticleRepo,
	clusterRepo repository.ClusterRepo,
	mqClient mq.Client) *env {
	return &env{
		config: config{
			TwitterUsers: 1000,
			MQ: mqConfig{
				Exchange:     "x-news",
				ScrapeQueue:  "q-scrape-targets",
				ScrapedQueue: "q-scraped-articles",
				RankQueue:    "q-rank-objects",
			},
		},
		articleRepo: articleRepo,
		clusterRepo: clusterRepo,
		mqClient:    mqClient,
	}
}

type mockClusterRepo struct {
	findByHashArg     string
	findByHashCluster domain.ArticleCluster
	findByHashErr     error

	saveArg    domain.ArticleCluster
	saveReturn error

	updateArg    domain.ArticleCluster
	updateReturn error
}

func (r *mockClusterRepo) FindByHash(arg string) (domain.ArticleCluster, error) {
	r.findByHashArg = arg
	return r.findByHashCluster, r.findByHashErr
}

func (r *mockClusterRepo) Save(arg domain.ArticleCluster) error {
	r.saveArg = arg
	return r.saveReturn
}

func (r *mockClusterRepo) Update(arg domain.ArticleCluster) error {
	r.updateArg = arg
	return r.updateReturn
}
