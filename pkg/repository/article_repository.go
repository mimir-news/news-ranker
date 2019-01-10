package repository

import (
	"database/sql"
	"strings"

	"github.com/mimir-news/pkg/dbutil"
	"github.com/mimir-news/pkg/schema/news"
	"github.com/pkg/errors"
)

const (
	keywordDelimiter = ","
)

// Common article repository errors.
var (
	ErrNoSuchArticle = errors.New("No such article")
	ErrNoSubjects    = errors.New("No subjects found")
	ErrNoReferers    = errors.New("No referers found")
	ErrFailedInsert  = errors.New("Insert failed")
)

// ArticleRepo data access interface for articles.
type ArticleRepo interface {
	FindByURL(url string) (news.Article, error)
	FindArticleSubjects(articleID string) ([]news.Subject, error)
	FindArticleReferers(articleID string) ([]news.Referer, error)
	Update(article news.Article) error
	SaveReferer(referer news.Referer) error
	SaveScrapedArticle(scrapedArticle news.ScrapedArticle) error
}

type pgArticleRepo struct {
	db *sql.DB
}

// NewArticleRepo creates a new ArticleRepo using the default implementation.
func NewArticleRepo(db *sql.DB) ArticleRepo {
	return &pgArticleRepo{
		db: db,
	}
}

const findArticleByURLQuery = `SELECT
  id, url, title, body, keywords, reference_score, article_date, created_at
  FROM article WHERE url = $1`

func (r *pgArticleRepo) FindByURL(url string) (news.Article, error) {
	var a news.Article
	var joinedKeywords sql.NullString
	err := r.db.QueryRow(findArticleByURLQuery, url).Scan(
		&a.ID, &a.URL, &a.Title, &a.Body, &joinedKeywords,
		&a.ReferenceScore, &a.ArticleDate, &a.CreatedAt)
	if err == sql.ErrNoRows {
		return a, errors.Wrap(ErrNoSuchArticle, "pgArticleRepo.FindByURL failed")
	} else if err != nil {
		return a, errors.Wrap(err, "pgArticleRepo.FindByURL failed")
	}
	a.Keywords = splitKeywords(joinedKeywords)
	return a, nil
}

const findArticleSubjectsQuery = `
  SELECT id, symbol, name, score, article_id FROM subject
  WHERE article_id = $1`

func (r *pgArticleRepo) FindArticleSubjects(articleID string) ([]news.Subject, error) {
	rows, err := r.db.Query(findArticleSubjectsQuery, articleID)
	if err == sql.ErrNoRows {
		return make([]news.Subject, 0), ErrNoSubjects
	} else if err != nil {
		return nil, errors.Wrap(err, "pgArticleRepo.FindArticleSubjects failed")
	}
	defer rows.Close()

	subjects, err := mapRowsToSubjects(rows)
	if err != nil {
		return nil, errors.Wrap(err, "pgArticleRepo.FindArticleSubjects failed")
	}
	return subjects, nil
}

func mapRowsToSubjects(rows *sql.Rows) ([]news.Subject, error) {
	subjects := make([]news.Subject, 0)
	for rows.Next() {
		var s news.Subject
		err := rows.Scan(&s.ID, &s.Symbol, &s.Name, &s.Score, &s.ArticleID)
		if err != nil {
			return nil, err
		}
		subjects = append(subjects, s)
	}
	return subjects, nil
}

const findArticleReferersQuery = `
  SELECT id, twitter_author, follower_count, article_id FROM twitter_references
  WHERE article_id = $1`

func (r *pgArticleRepo) FindArticleReferers(articleID string) ([]news.Referer, error) {
	rows, err := r.db.Query(findArticleReferersQuery, articleID)
	if err == sql.ErrNoRows {
		return make([]news.Referer, 0), ErrNoReferers
	} else if err != nil {
		return nil, errors.Wrap(err, "pgArticleRepo.FindArticleReferers failed")
	}
	defer rows.Close()

	referers, err := mapRowsToReferers(rows)
	if err != nil {
		return nil, errors.Wrap(err, "pgArticleRepo.FindArticleReferers failed")
	}
	return referers, nil
}

func mapRowsToReferers(rows *sql.Rows) ([]news.Referer, error) {
	referers := make([]news.Referer, 0)
	for rows.Next() {
		var r news.Referer
		err := rows.Scan(&r.ID, &r.ExternalID, &r.FollowerCount, &r.ArticleID)
		if err != nil {
			return nil, err
		}
		referers = append(referers, r)
	}
	return referers, nil
}

const updateArticleQuery = `
  UPDATE article SET reference_score = $1
  WHERE id = $2`

func (r *pgArticleRepo) Update(article news.Article) error {
	res, err := r.db.Exec(updateArticleQuery, article.ReferenceScore, article.ID)
	if err != nil {
		return errors.Wrap(err, "pgArticleRepo.Update failed")
	}

	var expectedUpdates int64 = 1
	return dbutil.AssertRowsAffected(res, expectedUpdates, ErrNoSuchArticle)
}

const insertReferencesQuery = `
  INSERT INTO twitter_references(id, twitter_author, follower_count, article_id)
  VALUES ($1, $2, $3, $4)`

func (r *pgArticleRepo) SaveReferer(referer news.Referer) error {
	res, err := r.db.Exec(
		insertReferencesQuery, referer.ID, referer.ExternalID,
		referer.FollowerCount, referer.ArticleID)
	if err != nil {
		return errors.Wrap(err, "pgArticleRepo.SaveReferer failed")
	}

	return dbutil.AssertRowsAffected(res, 1, ErrFailedInsert)
}

func (r *pgArticleRepo) SaveScrapedArticle(scrapedArticle news.ScrapedArticle) error {
	tx, err := r.db.Begin()
	if err != nil {
		return err
	}

	err = r.upsertArticle(scrapedArticle.Article, tx)
	if err != nil {
		dbutil.RollbackTx(tx)
		return err
	}

	err = r.insertReferer(scrapedArticle.Referer, tx)
	if err != nil {
		dbutil.RollbackTx(tx)
		return err
	}

	err = r.upsertSubjects(scrapedArticle.Subjects, tx)
	if err != nil {
		dbutil.RollbackTx(tx)
		return err
	}

	return tx.Commit()
}

const upsertArticleQuery = `
  INSERT INTO
  article(id, url, title, body, keywords, reference_score, article_date, created_at)
  VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
  ON CONFLICT ON CONSTRAINT article_pkey
  DO UPDATE SET reference_score = $6`

func (r *pgArticleRepo) upsertArticle(article news.Article, tx *sql.Tx) error {
	keywords := joinKeywords(article.Keywords)
	res, err := r.db.Exec(
		upsertArticleQuery,
		article.ID, article.URL, article.Title, article.Body, keywords,
		article.ReferenceScore, article.ArticleDate)
	if err != nil {
		return errors.Wrap(err, "pgArticleRepo.upsertArticle failed")
	}

	return dbutil.AssertRowsAffected(res, 1, ErrFailedInsert)
}

const insertReferencesIgnoreConflictsQuery = `
  INSERT INTO twitter_references(id, twitter_author, follower_count, article_id)
  VALUES ($1, $2, $3, $4)
  ON CONFLICT ON CONSTRAINT twitter_references_pkey DO NOTHING`

func (r *pgArticleRepo) insertReferer(referer news.Referer, tx *sql.Tx) error {
	_, err := r.db.Exec(
		insertReferencesIgnoreConflictsQuery,
		referer.ID, referer.ExternalID, referer.FollowerCount, referer.ArticleID)
	if err != nil {
		return errors.Wrap(err, "pgArticleRepo.insertReferer failed")
	}

	return nil
}

const upsertSubjectQuery = `
  INSERT INTO subject(id, symbol, name, score, article_id)
  VALUES ($1, $2, $3, $4, $5)
  ON CONFLICT ON CONSTRAINT subject_pkey DO UPDATE SET score = $4`

func (r *pgArticleRepo) upsertSubjects(subjects []news.Subject, tx *sql.Tx) error {
	for _, s := range subjects {
		_, err := tx.Exec(upsertSubjectQuery, s.ID, s.Symbol, s.Name, s.Score, s.ArticleID)
		if err != nil {
			return errors.Wrap(err, "pgArticleRepo.upsertSubjects failed")
		}
	}
	return nil
}

func joinKeywords(keywords []string) sql.NullString {
	if keywords == nil {
		return sql.NullString{}
	}

	return sql.NullString{
		Valid:  true,
		String: strings.Join(keywords, keywordDelimiter),
	}
}

func splitKeywords(keywords sql.NullString) []string {
	if !keywords.Valid {
		return make([]string, 0)
	}

	return strings.Split(keywords.String, keywordDelimiter)
}
