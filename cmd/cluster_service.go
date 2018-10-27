package main

import (
	"log"

	"github.com/mimir-news/news-ranker/pkg/domain"
	"github.com/mimir-news/news-ranker/pkg/repository"
	"github.com/mimir-news/pkg/schema/news"
)

func (e *env) clusterArticle(article news.Article) {
	subjects, err := e.articleRepo.FindArticleSubjects(article.ID)
	if err != nil && err != repository.ErrNoSubjects {
		log.Println(err)
		return
	}

	for _, subject := range subjects {
		e.clusterArticleWithSubject(article, subject)
	}
}

func (e *env) clusterArticleWithSubject(article news.Article, subject news.Subject) {
	clusterHash := domain.CalcClusterHash(article.Title, subject.Symbol, article.ArticleDate)

	cluster, err := e.clusterRepo.FindByHash(clusterHash)
	if err == repository.ErrNoSuchCluster {
		e.createNewCluster(clusterHash, article, subject)
		return
	}
	if err != nil {
		log.Println(err)
		return
	}

	e.updateArticleCluster(cluster, article, subject)
}

func (e *env) createNewCluster(clusterHash string, article news.Article, subject news.Subject) {
	members := createNewClusterMemebers(clusterHash, article, subject)

	cluster := domain.NewArticleCluster(
		article.Title, subject.Symbol, article.ArticleDate,
		article.ID, members[0].Score(), members)

	err := e.clusterRepo.Save(*cluster)
	if err != nil {
		log.Println(err)
	}
}

func (e *env) updateArticleCluster(cluster domain.ArticleCluster, article news.Article, subject news.Subject) {
	updateClusterMembers(&cluster, article, subject)
	cluster.ElectLeaderAndScore()

	err := e.clusterRepo.Update(cluster)
	if err != nil {
		log.Println(err)
	}
}

func updateClusterMembers(cluster *domain.ArticleCluster, article news.Article, subject news.Subject) {
	newMember := createNewClusterMember(cluster, article, subject)
	cluster.AddMember(newMember)
}

func createNewClusterMember(c *domain.ArticleCluster, a news.Article, s news.Subject) domain.ClusterMember {
	return *domain.NewClusterMember(c.Hash, a.ID, a.ReferenceScore, s.Score)
}

func createNewClusterMemebers(clusterHash string, article news.Article, subject news.Subject) []domain.ClusterMember {
	return []domain.ClusterMember{
		*domain.NewClusterMember(clusterHash, article.ID, article.ReferenceScore, subject.Score),
	}
}
