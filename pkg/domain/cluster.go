package domain

import (
	"crypto/sha256"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/mimir-news/pkg/id"
	"go.uber.org/zap"
)

const dateFormat = "2006-01-02"

var logger *zap.SugaredLogger

func init() {
	l, err := zap.NewProduction()
	if err != nil {
		log.Fatal("main.init zap.Logger init failed.", err)
	}

	logger = l.Sugar().With("package", "domain")
}

// ArticleCluster is a collection of articles.
type ArticleCluster struct {
	Hash          string
	Title         string
	Symbol        string
	ArticleDate   time.Time
	LeadArticleID string
	Score         float64
	Members       []ClusterMember
}

// AddMember add an additional member to the article cluster.
func (a *ArticleCluster) AddMember(newMember ClusterMember) {
	for _, member := range a.Members {
		if member.ArticleID == newMember.ArticleID {
			logger.Infow("Article already in cluster", "articleId", newMember.ArticleID, "clusterHash", a.Hash)
			return
		}
	}
	a.Members = append(a.Members, newMember)
}

// ElectLeaderAndScore finds highes scoring member and sums up the total cluster score.
func (a *ArticleCluster) ElectLeaderAndScore() {
	leader := selectHighestScoreMember(a.Members)
	referenceSum := sumReferenceScore(a.Members)
	a.LeadArticleID = leader.ArticleID
	a.Score = leader.SubjectScore + referenceSum
}

func selectHighestScoreMember(members []ClusterMember) ClusterMember {
	var highScoreMember ClusterMember
	highScore := 0.0
	for _, member := range members {
		if member.Score() >= highScore {
			highScore = member.Score()
			highScoreMember = member
		}
	}
	return highScoreMember
}

func sumReferenceScore(members []ClusterMember) float64 {
	var referenceSum float64
	for _, member := range members {
		referenceSum += member.ReferenceScore
	}
	return referenceSum
}

// NewArticleCluster creates a new article cluster.
func NewArticleCluster(title, symbol string, articleDate time.Time, leadArticleID string,
	score float64, members []ClusterMember) *ArticleCluster {
	return &ArticleCluster{
		Hash:          CalcClusterHash(title, symbol, articleDate),
		Title:         title,
		Symbol:        symbol,
		ArticleDate:   articleDate,
		LeadArticleID: leadArticleID,
		Score:         score,
		Members:       members,
	}
}

// CalcClusterHash calculates sha256 digest of a title, symbol and date.
func CalcClusterHash(title, symbol string, date time.Time) string {
	lowerTitle := strings.ToLower(title)
	lowerSymbol := strings.ToLower(symbol)
	dateStr := date.Format(dateFormat)
	byteHash := sha256.Sum256([]byte(lowerTitle + lowerSymbol + dateStr))
	return fmt.Sprintf("%x", byteHash)
}

// String returns a string representation of an article cluster.
func (a *ArticleCluster) String() string {
	return fmt.Sprintf(
		"ArticleCluster(hash=%s title=%s symbol=%s articleDate=%s leadArticleId=%s score=%f)",
		a.Hash, a.Title, a.Symbol, a.ArticleDate, a.LeadArticleID, a.Score)
}

// ClusterMember is a scored article that is part of a cluster.
type ClusterMember struct {
	ID             string
	ClusterHash    string
	ArticleID      string
	ReferenceScore float64
	SubjectScore   float64
}

// NewClusterMember creates a new ClusterMemeber
func NewClusterMember(clusterHash, articleID string, referenceScore, subjectScore float64) *ClusterMember {
	return &ClusterMember{
		ID:             id.New(),
		ClusterHash:    clusterHash,
		ArticleID:      articleID,
		ReferenceScore: referenceScore,
		SubjectScore:   subjectScore,
	}
}

// Score returns the compound score of a cluster member.
func (m *ClusterMember) Score() float64 {
	return m.ReferenceScore + m.SubjectScore
}

func (m *ClusterMember) String() string {
	return fmt.Sprintf(
		"ClusterMember(id=%s clusterHash=%s articleId=%s referenceScore=%f subjectScore=%f)",
		m.ID, m.ClusterHash, m.ArticleID, m.ReferenceScore, m.SubjectScore)
}
