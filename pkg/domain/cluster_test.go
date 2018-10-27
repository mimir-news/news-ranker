package domain

import (
	"testing"
	"time"

	"github.com/mimir-news/pkg/id"
)

func TestAddMember(t *testing.T) {
	title := "title"
	symbol := "symbol"
	articleDate := time.Now()
	clusterHash := CalcClusterHash(title, symbol, articleDate)
	members := []ClusterMember{
		*NewClusterMember(clusterHash, "member-1", 1.0, 1.0),
		*NewClusterMember(clusterHash, "member-2", 2.0, 2.0),
	}
	cluster := NewArticleCluster(title, symbol, articleDate, "", 0, members)

	newMember := *NewClusterMember(clusterHash, "member-3", 3.0, 3.0)
	cluster.AddMember(newMember)
	lastMember := cluster.Members[2]
	if newMember.String() != lastMember.String() {
		t.Errorf("ArticleCluster.AddMember failed. \nShould have added member=%s\nFound=%s",
			newMember.String(), lastMember.String())
	}

	existingMember := *NewClusterMember(clusterHash, "member-2", 2.0, 2.0)
	lengthBeforeAdd := len(cluster.Members)
	cluster.AddMember(existingMember)
	lengthAfterAdd := len(cluster.Members)
	if lengthBeforeAdd != lengthAfterAdd {
		t.Errorf("ArticleCluster.AddMember failed, incorrectly added existing member=%s",
			existingMember.String())
	}
}

func TestElectLeaderAndScore(t *testing.T) {
	title := "title"
	symbol := "symbol"
	articleDate := time.Now()
	clusterHash := CalcClusterHash(title, symbol, articleDate)
	members := []ClusterMember{
		*NewClusterMember(clusterHash, "member-1", 1.0, 1.0),
		*NewClusterMember(clusterHash, "member-2", 1.0, 2.0),
		*NewClusterMember(clusterHash, "member-3", 1.0, 3.0),
	}
	cluster1 := NewArticleCluster(title, symbol, articleDate, "", 0, members)

	cluster1.ElectLeaderAndScore()
	if cluster1.Score != 6.0 {
		t.Errorf("ArticleCluster.ElectLeaderAndScore wrong score. Expected=6.0 Actual=%f",
			cluster1.Score)
	}
	if cluster1.LeadArticleID != "member-3" {
		t.Errorf("ArticleCluster.ElectLeaderAndScore wrong LeadArticleId. Expected=member-3 Actual=%s",
			cluster1.LeadArticleID)
	}

	members2 := []ClusterMember{
		*NewClusterMember(clusterHash, "member-1", 1.0, 1.0),
		*NewClusterMember(clusterHash, "member-2", 1.0, 3.0),
		*NewClusterMember(clusterHash, "member-3", 1.0, 2.0),
	}
	cluster2 := NewArticleCluster(title, symbol, articleDate, "", 0, members2)

	cluster2.ElectLeaderAndScore()
	if cluster2.Score != 6.0 {
		t.Errorf("ArticleCluster.ElectLeaderAndScore wrong score. Expected=6.0 Actual=%f",
			cluster2.Score)
	}
	if cluster2.LeadArticleID != "member-2" {
		t.Errorf("ArticleCluster.ElectLeaderAndScore wrong LeadArticleId. Expected=member-2 Actual=%s",
			cluster2.LeadArticleID)
	}
}

func TestCalcClusterHash(t *testing.T) {
	title := "title"
	symbol := "symbol"
	date, err := time.Parse(dateFormat, "2018-09-30")
	if err != nil {
		t.Fatalf("Unexpecetd parsing error: %s", err.Error())
	}

	exectedHash := "d81010615f4d61a196669ce23f3f416af29043daaf1432ba1449254317667d68"
	actualHash := CalcClusterHash(title, symbol, date)
	if exectedHash != actualHash {
		t.Errorf("CalcClusterHash failed with lower case arguments.\nExpected=%s\nActual=%s",
			exectedHash, actualHash)
	}

	title = "TITLE"
	symbol = "SYMBOL"
	actualHash = CalcClusterHash(title, symbol, date)
	if exectedHash != actualHash {
		t.Errorf("CalcClusterHash failed uppercase arguments.\nExpected=%s\nActual=%s",
			exectedHash, actualHash)
	}
}

func TestClusterMemberScore(t *testing.T) {
	member := NewClusterMember("hash", id.New(), 1.0, 1.0)
	score := member.Score()
	if score != 2.0 {
		t.Errorf("ClusterMember.Score failed. Expected=2.0 Actual=%f", score)
	}
}
