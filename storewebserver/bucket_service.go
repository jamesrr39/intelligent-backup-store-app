package storewebserver

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
)

type BucketService struct {
	store  *intelligentstore.IntelligentStore
	router *mux.Router
}

func NewBucketService(store *intelligentstore.IntelligentStore) *BucketService {
	router := mux.NewRouter()
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		buckets, err := store.GetAllBuckets()
		if nil != err {
			http.Error(w, err.Error(), 500)
			return
		}

		log.Println("bucket listing")
		w.Header().Set("Content-Type", "application/json")

		err = json.NewEncoder(w).Encode(buckets)
		if nil != err {
			http.Error(w, err.Error(), 500)
			return
		}
	})

	router.HandleFunc("/{bucketName}", func(w http.ResponseWriter, r *http.Request) {
		bucketName := mux.Vars(r)["bucketName"]

		bucket, err := store.GetBucket(bucketName)
		if nil != err {
			http.Error(w, err.Error(), 500) //TODO error code 404
			return
		}

		revisions, err := bucket.GetRevisions()
		if nil != err {
			http.Error(w, err.Error(), 500)
			return
		}

		err = json.NewEncoder(w).Encode(revisions)
		if nil != err {
			http.Error(w, err.Error(), 500)
			return
		}
	})

	return &BucketService{store, router}
}

func (s *BucketService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}
