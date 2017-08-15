package storewebserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"

	"github.com/jamesrr39/intelligent-backup-store-app/serialisation"
)

// BucketService handles HTTP requests to get bucket information.
type BucketService struct {
	store  *intelligentstore.IntelligentStore
	router *mux.Router
	openTransactionsMap
}

type openTransactionsMap map[string]*intelligentstore.Transaction

// NewBucketService creates a new BucketService and a router for handling requests.
func NewBucketService(store *intelligentstore.IntelligentStore) *BucketService {
	router := mux.NewRouter()
	bucketService := &BucketService{store, router, make(openTransactionsMap)}

	router.HandleFunc("/", bucketService.handleGetAll)
	router.HandleFunc("/{bucketName}", bucketService.handleGet).Methods("GET")
	router.HandleFunc("/{bucketName}/upload", bucketService.handleCreateRevision).Methods("GET")
	router.HandleFunc("/{bucketName}/upload/{revisionTs}/file", bucketService.handleUploadFile).Methods("POST")
	router.HandleFunc("/{bucketName}/upload/{revisionTs}/commit", bucketService.handleCommitTransaction).Methods("GET")

	router.HandleFunc("/{bucketName}/{revisionTs}", bucketService.handleGetRevision).Methods("GET")

	return bucketService
}

type bucketSummary struct {
	Name           string `json:"name"`
	LastRevisionTs int64  `json:"lastRevisionTs"`
}

type revisionInfoWithFiles struct {
	LastRevisionTs int64                    `json:"revisionTs"`
	Files          []*intelligentstore.File `json:"files"`
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
	var latestRevision *intelligentstore.Revision
	var latestRevisionTs int64
	for _, bucket := range buckets {
		latestRevision, err = bucket.GetLatestRevision()
		if nil != err {
			http.Error(w, err.Error(), 500)
			return
		}

		latestRevisionTs, err = strconv.ParseInt(latestRevision.VersionTimestamp, 10, 64)
		if nil != err {
			http.Error(w, err.Error(), 500)
			return
		}
		bucketsSummaries = append(bucketsSummaries, &bucketSummary{bucket.BucketName, latestRevisionTs})
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

func (s *BucketService) handleGetRevision(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	bucketName := vars["bucketName"]
	revisionTsString := vars["revisionTs"]

	bucket, err := s.store.GetBucket(bucketName)
	if nil != err {
		if intelligentstore.ErrBucketDoesNotExist == err {
			http.Error(w, fmt.Sprintf("couldn't find bucket '%s'. Error: %s", bucketName, err), 404)
			return
		}
		http.Error(w, err.Error(), 500)
		return
	}

	var revision *intelligentstore.Revision
	if "latest" == revisionTsString {
		revision, err = bucket.GetLatestRevision()
		if nil != err {
			http.Error(w, err.Error(), 500)
			return
		}
	} else {
		panic("not implemented yet")
	}

	revisionTs, err := strconv.ParseInt(revision.VersionTimestamp, 10, 64)
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}

	files, err := revision.GetFilesInRevision()
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}

	data := &revisionInfoWithFiles{revisionTs, files}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(data)
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}
}

func (s *BucketService) handleCreateRevision(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	bucketName := vars["bucketName"]

	bucket, err := s.store.GetBucket(bucketName)
	if nil != err {
		if intelligentstore.ErrBucketDoesNotExist == err {
			http.Error(w, fmt.Sprintf("couldn't find bucket '%s'. Error: %s", bucketName, err), 404)
			return
		}
		http.Error(w, err.Error(), 500)
		return
	}

	transaction := bucket.Begin()
	s.openTransactionsMap[bucket.BucketName+"__"+transaction.VersionTimestamp] = transaction

	w.Write([]byte(transaction.VersionTimestamp))
}

func (s *BucketService) handleUploadFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	bucketName := vars["bucketName"]
	revisionTsString := vars["revisionTs"]

	bucket, err := s.store.GetBucket(bucketName)
	if nil != err {
		if intelligentstore.ErrBucketDoesNotExist == err {
			http.Error(w, fmt.Sprintf("couldn't find bucket '%s'. Error: %s", bucketName, err), 404)
			return
		}
		http.Error(w, err.Error(), 500)
		return
	}

	transaction := s.openTransactionsMap[bucket.BucketName+"__"+revisionTsString]
	if nil == transaction {
		http.Error(w, fmt.Sprintf("there is no open transaction for bucket %s and revisionTs %s", bucket.BucketName, revisionTsString), 400)
		return
	}

	bodyBytes, err := ioutil.ReadAll(r.Body)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't read response body. Error: '%s'", err), 400)
		return
	}
	defer r.Body.Close()

	var uploadedFile serialisation.UploadedFile
	err = proto.Unmarshal(bodyBytes, &uploadedFile)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't unmarshall message. Error: '%s'", err), 400)
	}

	err = transaction.BackupFile(uploadedFile.Filename, bytes.NewBuffer(uploadedFile.File))
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}
}

func (s *BucketService) handleCommitTransaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	bucketName := vars["bucketName"]
	revisionTsString := vars["revisionTs"]

	bucket, err := s.store.GetBucket(bucketName)
	if nil != err {
		if intelligentstore.ErrBucketDoesNotExist == err {
			http.Error(w, fmt.Sprintf("couldn't find bucket '%s'. Error: %s", bucketName, err), 404)
			return
		}
		http.Error(w, err.Error(), 500)
		return
	}

	transaction := s.openTransactionsMap[bucketName+"__"+revisionTsString]
	if nil == transaction {
		http.Error(w, fmt.Sprintf("there is no open transaction for bucket %s and revisionTs %s", bucket.BucketName, revisionTsString), 400)
		return
	}

	err = transaction.Commit()
	if nil != err {
		http.Error(w, "failed to commit transaction. Error: "+err.Error(), 500)
		return
	}
	s.openTransactionsMap[bucketName+"__"+revisionTsString] = nil
}

func (s *BucketService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}
