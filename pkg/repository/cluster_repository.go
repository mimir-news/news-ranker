package repository

import (
	"database/sql"

	"github.com/mimir-news/news-ranker/pkg/domain"
	"github.com/mimir-news/pkg/dbutil"
	"github.com/pkg/errors"
)

// Common cluster repository errors.
var (
	ErrNoSuchCluster = errors.New("no such cluster")
	ErrUpdateFailed  = errors.New("Update failed")
)

// ClusterRepo data access interface for article clusters.
type ClusterRepo interface {
	FindByHash(clusterHash string) (domain.ArticleCluster, error)
	Save(cluster domain.ArticleCluster) error
	Update(cluster domain.ArticleCluster) error
}

type pgClusterRepo struct {
	db *sql.DB
}

// NewClusterRepo creates a new ClusterRepo using the default implementation.
func NewClusterRepo(db *sql.DB) ClusterRepo {
	return &pgClusterRepo{
		db: db,
	}
}

func (r *pgClusterRepo) FindByHash(clusterHash string) (domain.ArticleCluster, error) {
	tx, err := r.db.Begin()
	if err != nil {
		return domain.ArticleCluster{}, errors.Wrap(err, "pgClusterRepo.FindByHash failed")
	}

	members, err := r.findClusterMembers(clusterHash, tx)
	if err != nil {
		dbutil.RollbackTx(tx)
		return domain.ArticleCluster{}, err
	}

	cluster, err := r.findCluster(clusterHash, tx)
	if err != nil {
		dbutil.RollbackTx(tx)
		return domain.ArticleCluster{}, err
	}

	cluster.Members = members
	return cluster, tx.Commit()
}

const findClusterMembersQuery = `
  SELECT id, reference_score, subject_score, cluster_hash, article_id
  FROM cluster_member WHERE cluster_hash = $1`

func (r *pgClusterRepo) findClusterMembers(clusterHash string, tx *sql.Tx) ([]domain.ClusterMember, error) {
	rows, err := tx.Query(findClusterMembersQuery, clusterHash)
	if err == sql.ErrNoRows {
		return nil, ErrNoSuchCluster
	} else if err != nil {
		return nil, errors.Wrap(err, "pgClusterRepo.findClusterMembers failed")
	}
	defer rows.Close()

	members, err := mapRowsToClusterMemebers(rows)
	if err != nil {
		return nil, errors.Wrap(err, "pgClusterRepo.findClusterMembers failed")
	}
	return members, rows.Err()
}

func mapRowsToClusterMemebers(rows *sql.Rows) ([]domain.ClusterMember, error) {
	members := make([]domain.ClusterMember, 0)
	for rows.Next() {
		var m domain.ClusterMember
		err := rows.Scan(&m.ID, &m.ReferenceScore, &m.SubjectScore, &m.ClusterHash, &m.ArticleID)
		if err != nil {
			return nil, err
		}
		members = append(members, m)
	}
	return members, nil
}

const findClusterQuery = `
  SELECT cluster_hash, title, symbol, article_date, score, lead_article_id
  FROM article_cluster WHERE cluster_hash = $1`

func (r *pgClusterRepo) findCluster(clusterHash string, tx *sql.Tx) (domain.ArticleCluster, error) {
	var c domain.ArticleCluster
	err := tx.QueryRow(findClusterQuery, clusterHash).Scan(
		&c.Hash, &c.Title, &c.Symbol, &c.ArticleDate, &c.Score, &c.LeadArticleID)
	if err == sql.ErrNoRows {
		return domain.ArticleCluster{}, ErrNoSuchCluster
	} else if err != nil {
		return domain.ArticleCluster{}, errors.Wrap(err, "pgClusterRepo.findCluster failed")
	}
	return c, nil
}

func (r *pgClusterRepo) Update(cluster domain.ArticleCluster) error {
	tx, err := r.db.Begin()
	if err != nil {
		return errors.Wrap(err, "pgClusterRepo.Update failed")
	}

	err = updateCluster(cluster, tx)
	if err != nil {
		dbutil.RollbackTx(tx)
		return err
	}

	err = upsertClusterMembers(cluster.Members, tx)
	if err != nil {
		dbutil.RollbackTx(tx)
		return err
	}

	return tx.Commit()
}

const updateClusterQuery = `
  UPDATE article_cluster SET
    score = $1, lead_article_id = $2
    WHERE cluster_hash = $3`

func updateCluster(cluster domain.ArticleCluster, tx *sql.Tx) error {
	res, err := tx.Exec(updateClusterQuery, cluster.Score, cluster.LeadArticleID, cluster.Hash)
	if err != nil {
		return errors.Wrap(err, "updateCluster failed")
	}
	return dbutil.AssertRowsAffected(res, 1, ErrUpdateFailed)
}

func (r *pgClusterRepo) Save(cluster domain.ArticleCluster) error {
	tx, err := r.db.Begin()
	if err != nil {
		return err
	}

	err = saveCluster(cluster, tx)
	if err != nil {
		dbutil.RollbackTx(tx)
		return err
	}

	err = upsertClusterMembers(cluster.Members, tx)
	if err != nil {
		dbutil.RollbackTx(tx)
		return err
	}

	return tx.Commit()
}

const saveClusterQuery = `
  INSERT INTO article_cluster(
    cluster_hash, title, symbol, article_date, score, lead_article_id
  ) VALUES ($1, $2, $3, $4, $5, $6)`

func saveCluster(cluster domain.ArticleCluster, tx *sql.Tx) error {
	res, err := tx.Exec(
		saveClusterQuery, cluster.Hash, cluster.Title, cluster.Symbol,
		cluster.ArticleDate, cluster.Score, cluster.LeadArticleID)
	if err != nil {
		return ErrFailedInsert
	}

	return dbutil.AssertRowsAffected(res, 1, ErrFailedInsert)
}

const upsertClusterMembersQuery = `
  INSERT INTO cluster_member(
    id, reference_score, subject_score, cluster_hash, article_id
  ) VALUES ($1, $2, $3, $4, $5)
  ON CONFLICT ON CONSTRAINT cluster_member_pkey 
  DO UPDATE SET reference_score = $2, subject_score = $3`

func upsertClusterMembers(members []domain.ClusterMember, tx *sql.Tx) error {
	for _, m := range members {
		res, err := tx.Exec(upsertClusterMembersQuery, m.ID, m.ReferenceScore, m.SubjectScore, m.ClusterHash, m.ArticleID)
		if err != nil {
			return errors.Wrap(err, "upsertClusterMembers failed")
		}
		err = dbutil.AssertRowsAffected(res, 1, ErrFailedInsert)
		if err != nil {
			return err
		}
	}
	return nil
}
