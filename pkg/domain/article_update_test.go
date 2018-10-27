package domain

import (
	"testing"
	"time"

	"github.com/mimir-news/pkg/schema/news"
)

func TestCreateArticleUpdate(t *testing.T) {
	a := news.Article{ID: "article-0"}
	oldSubj := []news.Subject{
		news.Subject{
			ID:        "some-id-0",
			Symbol:    "s-0",
			ArticleID: "article-0",
		},
		news.Subject{
			ID:        "some-id-1",
			Symbol:    "s-1",
			ArticleID: "article-0",
		},
	}
	newSubj := []news.Subject{
		news.Subject{Symbol: "s-2"},
	}
	repeatedSubjects := []news.Subject{
		news.Subject{Symbol: "s-1"},
	}
	mergedSubjects := []news.Subject{
		news.Subject{
			ID:        "some-id-0",
			Symbol:    "s-0",
			ArticleID: "article-0",
		},
		news.Subject{
			ID:        "some-id-0",
			Symbol:    "s-1",
			ArticleID: "article-0",
		},
		news.Subject{
			ID:        "some-id-will-not-be-this-but-must-be-set",
			Symbol:    "s-2",
			ArticleID: "article-0",
		},
	}

	oldRefs := []news.Referer{
		news.Referer{
			ID:         "some-id-0",
			ExternalID: "r-0",
			ArticleID:  "article-0",
		},
		news.Referer{
			ID:         "some-id-0",
			ExternalID: "r-1",
			ArticleID:  "article-0",
		},
	}
	newRef := news.Referer{ExternalID: "r-2"}
	repeatedRef := news.Referer{ExternalID: "r-1"}
	mergedRefs := []news.Referer{
		news.Referer{
			ID:         "some-id-0",
			ExternalID: "r-0",
			ArticleID:  "article-0",
		},
		news.Referer{
			ID:         "some-id-0",
			ExternalID: "r-1",
			ArticleID:  "article-0",
		},
		news.Referer{
			ID:         "some-id-will-not-be-this-but-must-be-set",
			ExternalID: "r-2",
			ArticleID:  "article-0",
		},
	}

	u1 := CreateArticleUpdate(a, oldSubj, repeatedSubjects, oldRefs, repeatedRef)
	assertArticleUpdate(t, u1, a, oldSubj, oldRefs)
	if u1.Type != NO_UPDATE {
		t.Errorf("CreateArticleUpdate failed. Expected type: %d Got: %d",
			NO_UPDATE, u1.Type)
	}

	u2 := CreateArticleUpdate(a, oldSubj, newSubj, oldRefs, repeatedRef)
	assertArticleUpdate(t, u2, a, mergedSubjects, oldRefs)
	if u2.Type != NEW_SUBJECTS {
		t.Errorf("CreateArticleUpdate failed. Expected type: %d Got: %d",
			NEW_SUBJECTS, u2.Type)
	}

	u3 := CreateArticleUpdate(a, oldSubj, repeatedSubjects, oldRefs, newRef)
	assertArticleUpdate(t, u3, a, oldSubj, mergedRefs)
	if u3.Type != NEW_REFERENCES {
		t.Errorf("CreateArticleUpdate failed. Expected type: %d Got: %d",
			NEW_REFERENCES, u3.Type)
	}

	u4 := CreateArticleUpdate(a, oldSubj, newSubj, oldRefs, newRef)
	assertArticleUpdate(t, u4, a, mergedSubjects, mergedRefs)
	if u4.Type != NEW_SUBJECTS_AND_REFERENCES {
		t.Errorf("CreateArticleUpdate failed. Expected type: %d Got: %d",
			NEW_SUBJECTS_AND_REFERENCES, u4.Type)
	}
}

func assertArticleUpdate(t *testing.T, u ArticleUpdate, eA news.Article, eS []news.Subject, eR []news.Referer) {
	if u.Article.ID != eA.ID {
		t.Errorf("Article.ID wrong. Expected: %s Got: %s", u.Article.ID, eA.ID)
	}

	if len(u.Subjects) != len(eS) {
		t.Fatalf("Subjects length missmatch. Expected: %d Got: %d", len(u.Subjects), len(eS))
	}
	for i, sub := range u.Subjects {
		if sub.Symbol != eS[i].Symbol {
			t.Errorf("%d - Subject.Symbol wrong. Expected: %s Got: %s", i, eS[i].Symbol, sub.Symbol)
		}
		if sub.ArticleID != eA.ID {
			t.Errorf("%d Wrong ArticleID on Subject. Expected: %s Got: %s", i, eA.ID, sub.ArticleID)
		}
		if sub.ID == "" {
			t.Errorf("%d ID not set on subject: %s", i, sub)
		}
	}

	if len(u.Referers) != len(eR) {
		t.Fatalf("References length missmatch. Expected: %d Got: %d", len(u.Referers), len(eR))
	}
	for i, ref := range u.Referers {
		if ref.ExternalID != eR[i].ExternalID {
			t.Errorf("%d - Referer.ExternalID wrong. Expected: %s Got: %s", i, eR[i].ExternalID, ref.ExternalID)
		}
		if ref.ArticleID != eA.ID {
			t.Errorf("%d Wrong ArticleID on Referer. Expected: %s Got: %s", i, eA.ID, ref.ArticleID)
		}
		if ref.ID == "" {
			t.Errorf("%d ID not set on referer: %s", i, ref)
		}
	}

	if u.NewReferer.ArticleID != eA.ID {
		t.Errorf("Wrong ArticleID on NewReferer. Expected: %s Got: %s", eA.ID, u.NewReferer.ArticleID)
	}
	if u.NewReferer.ID == "" {
		t.Errorf("ID not set on NewReferer: %s", u.NewReferer)
	}
}

func TestToScrapeTarget(t *testing.T) {
	articleTime, _ := time.Parse("2006-01-02", "2018-10-20")
	update := ArticleUpdate{
		Type: NEW_SUBJECTS,
		Article: news.Article{
			ID:    "a-id",
			URL:   "a-url",
			Title: "a-title",
			Body:  "a-body",
			Keywords: []string{
				"k-0",
				"k-1",
			},
			ReferenceScore: 0.5,
			ArticleDate:    articleTime,
			CreatedAt:      articleTime,
		},
		Subjects: []news.Subject{
			news.Subject{Symbol: "s-0"},
			news.Subject{Symbol: "s-1"},
		},
		Referers: []news.Referer{
			news.Referer{ExternalID: "r-0"},
			news.Referer{ExternalID: "r-1"},
		},
		NewReferer: news.Referer{ExternalID: "r-1"},
	}

	exptectedTarget := news.ScrapeTarget{
		URL: "a-url",
		Subjects: []news.Subject{
			news.Subject{Symbol: "s-0"},
			news.Subject{Symbol: "s-1"},
		},
		Referer:        news.Referer{ExternalID: "r-1"},
		ReferenceScore: 0.5,
		Title:          "a-title",
		Body:           "a-body",
		ArticleID:      "a-id",
	}

	actualTarget := update.ToScapeTarget()

	if actualTarget.String() != exptectedTarget.String() {
		t.Errorf(
			"ArticleUpdate.ToScapeTarget failed.\nExpected=%s\nGot=%s",
			exptectedTarget, actualTarget)
	}
}
