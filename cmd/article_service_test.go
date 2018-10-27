package main

import "github.com/mimir-news/pkg/schema/news"

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
