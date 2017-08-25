package storewebserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/serialisation"
	"github.com/jamesrr39/intelligent-backup-store-app/serialisation/protogenerated"
)

// BucketService handles HTTP requests to get bucket information.
type BucketService struct {
	store  *intelligentstore.IntelligentStore
	router *mux.Router
	openTransactionsMap
}

type openTransactionsMap map[string]*intelligentstore.Transaction

type subDirInfo struct {
	Name            string `json:"name"`
	NestedFileCount int64  `json:"nestedFileCount"`
}

// NewBucketService creates a new BucketService and a router for handling requests.
func NewBucketService(store *intelligentstore.IntelligentStore) *BucketService {
	router := mux.NewRouter()
	bucketService := &BucketService{store, router, make(openTransactionsMap)}

	router.HandleFunc("/", bucketService.handleGetAll)
	router.HandleFunc("/{bucketName}", bucketService.handleGet).Methods("GET")
	router.HandleFunc("/{bucketName}/upload", bucketService.handleCreateRevision).Methods("POST")
	router.HandleFunc("/{bucketName}/upload/{revisionTs}/file", bucketService.handleUploadFile).Methods("POST")
	router.HandleFunc("/{bucketName}/upload/{revisionTs}/commit", bucketService.handleCommitTransaction).Methods("GET")

	router.PathPrefix("/{bucketName}/{revisionTs}").HandlerFunc(bucketService.handleGetRevision).Methods("GET")
	return bucketService
}

type bucketSummary struct {
	Name           string `json:"name"`
	LastRevisionTs int64  `json:"lastRevisionTs"`
}

type revisionInfoWithFiles struct {
	LastRevisionTs int64                              `json:"revisionTs"`
	Files          []*intelligentstore.FileDescriptor `json:"files"`
	Dirs           []*subDirInfo                      `json:"dirs"`
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

	allFiles, err := revision.GetFilesInRevision()
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}

	rootDir, err := url.QueryUnescape(strings.TrimPrefix(r.URL.Query().Get("rootDir"), "/"))
	if nil != err {
		http.Error(w, "couldn't unescape rootDir. Error: "+err.Error(), 400)
		return
	}
	log.Printf("rootdir: %s\n", rootDir)
	files := []*intelligentstore.FileDescriptor{}

	type subDirInfoMap map[string]int64 // map[name]nestedFileCount
	dirnames := subDirInfoMap{}         // dirname[nested file count]
	for _, file := range allFiles {
		if !strings.HasPrefix(file.FilePath, rootDir) {
			// not in this root dir
			continue
		}

		relativeFilePath := strings.TrimPrefix(strings.TrimPrefix(file.FilePath, rootDir), "/")

		indexOfSlash := strings.Index(relativeFilePath, "/")
		if indexOfSlash != -1 {
			// file is inside a dir (not in the root folder)
			dirnames[relativeFilePath[0:indexOfSlash]]++
		} else {
			// file is in the dir we're searching inside
			files = append(files, file)
		}
	}

	dirs := []*subDirInfo{}
	for dirname, nestedFileCount := range dirnames {
		dirs = append(dirs, &subDirInfo{dirname, nestedFileCount})
	}

	data := &revisionInfoWithFiles{revisionTs, files, dirs}

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

	requestBytes, err := ioutil.ReadAll(r.Body)
	if nil != err {
		http.Error(w, "couldn't read request body. Error: "+err.Error(), 400)
		return
	}

	var openTxRequest protogenerated.OpenTxRequest
	err = proto.Unmarshal(requestBytes, &openTxRequest)
	if nil != err {
		http.Error(w, "couldn't unmarshal proto of request body. Error: "+err.Error(), 400)
		return
	}

	fileDetectorsForRequiredFiles := &protogenerated.FileDescriptorProtoList{}
	var hashAlreadyExists bool

	for _, fileDescriptorProto := range openTxRequest.FileDescriptorList.FileDescriptorList {
		fileDescriptor := serialisation.FileDescriptorProtoToFileDescriptor(fileDescriptorProto)

		hashAlreadyExists, err = transaction.AddAlreadyExistingHash(fileDescriptor)
		if nil != err {
			http.Error(w, fmt.Sprintf("error detecting if a hash for %s (%s) already exists", fileDescriptor.Hash, fileDescriptor.FilePath), 500)
			return
		}

		// if a file for the hash already exists, the transaction adds it to the files in version and we don't need it from the client
		if !hashAlreadyExists {
			fileDetectorsForRequiredFiles.FileDescriptorList = append(
				fileDetectorsForRequiredFiles.FileDescriptorList,
				fileDescriptorProto)
		}
	}

	openTxReponse := &protogenerated.OpenTxResponse{
		RevisionStr:        transaction.VersionTimestamp,
		FileDescriptorList: fileDetectorsForRequiredFiles,
	}

	responseBytes, err := proto.Marshal(openTxReponse)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't marshall response for the files the server requires. Error: %s", err), 500)
		return
	}

	_, err = w.Write([]byte(responseBytes))
	if nil != err {
		log.Printf("failed to send a response back to the client for files required to open transaction. Bucket: '%s', Revision: '%s'. Error: %s\n", bucketName, transaction.VersionTimestamp, err)
	}
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

	var uploadedFile protogenerated.FileProto
	err = proto.Unmarshal(bodyBytes, &uploadedFile)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't unmarshal message. Error: '%s'", err), 400)
	}

	err = transaction.BackupFile(
		uploadedFile.Descriptor_.Filename,
		bytes.NewBuffer(uploadedFile.Contents))
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
