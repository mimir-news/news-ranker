-- +migrate Up
CREATE TABLE article (
  id VARCHAR(50) PRIMARY KEY,
  url VARCHAR(350) NOT NULL,
  title VARCHAR(350) NOT NULL,
  body TEXT,
  keywords TEXT,
  reference_score NUMERIC(9,5) NOT NULL,
  article_date DATE,
  created_at TIMESTAMP,
  UNIQUE (url)
);

CREATE TABLE twitter_references (
  id VARCHAR(50) PRIMARY KEY,
  twitter_author VARCHAR(50),
  follower_count INT,
  article_id REFERENCES article(id),
  UNIQUE (twitter_author, article_id)
)

CREATE TABLE subject (
  id VARCHAR(50) PRIMARY KEY,
  symbol VARCHAR(15),
  name VARCHAR(50),
  score NUMERIC(9,5) NOT NULL,
  article_id REFERENCES article(id),
  UNIQUE (symbol, article_id)
);

CREATE TABLE article_cluster (
  cluster_hash VARCHAR(64) PRIMARY KEY,
  title VARCHAR(255),
  symbol VARCHAR(15),
  article_date DATE,
  score NUMERIC(9,5),
  lead_article_id VARCHAR(50) REFERENCES article(id),
);

CREATE TABLE cluster_member (
  id VARCHAR(50) PRIMARY KEY,
  reference_score NUMERIC(9,5),
  subject_score NUMERIC(9,5),
  cluster_hash VARCHAR(64) REFERENCES article_cluster(cluster_hash),
  article_id VARCHAR(50) REFERENCES article(id),
  UNIQUE (cluster_hash, article_id)
);

-- +migrate Down
DROP TABLE IF EXISTS cluster_member;
DROP TABLE IF EXISTS article_cluster;
DROP TABLE IF EXISTS subject;
DROP TABLE IF EXISTS compound_score;
DROP TABLE IF EXISTS twitter_references;
DROP TABLE IF EXISTS article;
