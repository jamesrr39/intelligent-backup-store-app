package storewebserver

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
)

// BucketService handles HTTP requests to get bucket information.
type BucketService struct {
	store  *intelligentstore.IntelligentStore
	router *mux.Router
}

// NewBucketService creates a new BucketService and a router for handling requests.
func NewBucketService(store *intelligentstore.IntelligentStore) *BucketService {
	router := mux.NewRouter()
	bucketService := &BucketService{store, router}

	router.HandleFunc("/", bucketService.handleGetAll)
	router.HandleFunc("/{bucketName}", bucketService.handleGet)

	return bucketService
}

type bucketSummary struct {
	Name           string `json:"name"`
	LastRevisionTs int64  `json:"lastRevisionTs"`
}

func (s *BucketService) handleGetAll(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.store.GetAllBuckets()
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if 0 == len(buckets) {
		w.Write([]byte("[]"))
		return
	}

	var bucketsSummaries []*bucketSummary
	var latestRevisionTime *time.Time
	for _, bucket := range buckets {
		latestRevisionTime, err = bucket.GetLatestVersionTime()
		if nil != err {
			http.Error(w, err.Error(), 500)
			return
		}
		bucketsSummaries = append(bucketsSummaries, &bucketSummary{bucket.BucketName, latestRevisionTime.Unix()})
	}

	err = json.NewEncoder(w).Encode(bucketsSummaries)
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}
}

func (s *BucketService) handleGet(w http.ResponseWriter, r *http.Request) {
	bucketName := mux.Vars(r)["bucketName"]

	bucket, err := s.store.GetBucket(bucketName)
	if nil != err {
		http.Error(w, err.Error(), 500) //TODO error code 404
		return
	}

	revisionsTimestamps, err := bucket.GetRevisionsTimestamps()
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}

	err = json.NewEncoder(w).Encode(revisionsTimestamps)
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}
}

func (s *BucketService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}
