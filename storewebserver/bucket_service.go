package storewebserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/protobufs"
	protofiles "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/protobufs/proto_files"
)

// BucketService handles HTTP requests to get bucket information.
type BucketService struct {
	store *intelligentstore.IntelligentStore
	http.Handler
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

	// swagger:route GET /api/buckets/ bucket listBuckets
	//     Produces:
	//     - application/json
	router.HandleFunc("/", bucketService.handleGetAllBuckets)

	// swagger:route GET /api/buckets/{bucketName} bucket getBucket
	//     Produces:
	//     - application/json
	router.HandleFunc("/{bucketName}", bucketService.handleGetBucket).Methods("GET")

	router.HandleFunc("/{bucketName}/upload", bucketService.handleCreateRevision).Methods("POST")
	router.HandleFunc("/{bucketName}/upload/{revisionTs}/file", bucketService.handleUploadFile).Methods("POST")
	router.HandleFunc("/{bucketName}/upload/{revisionTs}/commit", bucketService.handleCommitTransaction).Methods("GET")

	router.HandleFunc("/{bucketName}/{revisionTs}", bucketService.handleGetRevision).Methods("GET")
	return bucketService
}

type bucketSummary struct {
	Name           string                            `json:"name"`
	LastRevisionTs *intelligentstore.RevisionVersion `json:"lastRevisionTs"`
}

type revisionInfoWithFiles struct {
	LastRevisionTs intelligentstore.RevisionVersion   `json:"revisionTs"`
	Files          []*intelligentstore.FileDescriptor `json:"files"`
	Dirs           []*subDirInfo                      `json:"dirs"`
}

// @Title Get Latest Buckets Information
// @Success 200 {object} string &quot;Success&quot;
func (s *BucketService) handleGetAllBuckets(w http.ResponseWriter, r *http.Request) {
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
	var latestRevisionTs *intelligentstore.RevisionVersion
	for _, bucket := range buckets {
		latestRevision, err = bucket.GetLatestRevision()
		if nil != err {
			if intelligentstore.ErrNoRevisionsForBucket != err {
				http.Error(w, err.Error(), 500)
				return
			}
		} else {
			latestRevisionTs = &latestRevision.VersionTimestamp
		}

		bucketsSummaries = append(bucketsSummaries, &bucketSummary{bucket.BucketName, latestRevisionTs})
	}

	err = json.NewEncoder(w).Encode(bucketsSummaries)
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}
}

type handleGetBucketResponse struct {
	Revisions []*intelligentstore.Revision `json:"revisions"`
}

func (s *BucketService) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := mux.Vars(r)["bucketName"]

	bucket, err := s.store.GetBucket(bucketName)
	if nil != err {
		if intelligentstore.ErrBucketDoesNotExist == err {
			http.Error(w, err.Error(), 404)
			return
		}
		http.Error(w, err.Error(), 500)
		return
	}

	revisions, err := bucket.GetRevisions()
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Set("content-type", "application/json")

	if 0 == len(revisions) {
		revisions = make([]*intelligentstore.Revision, 0)
	}

	sort.Slice(revisions, func(i int, j int) bool {
		if revisions[i].VersionTimestamp < revisions[j].VersionTimestamp {
			return true
		}
		return false
	})

	err = json.NewEncoder(w).Encode(handleGetBucketResponse{revisions})
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
	} else {
		var revisionTimestamp int64
		revisionTimestamp, err = strconv.ParseInt(revisionTsString, 10, 64)
		if nil != err {
			http.Error(w, fmt.Sprintf("couldn't convert '%s' to a timestamp. Error: '%s'", revisionTsString, err), 400)
			return
		}
		revision, err = bucket.GetRevision(revisionTimestamp)
	}
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}

	allFiles, err := revision.GetFilesInRevision()
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}

	rootDir, err := url.QueryUnescape(r.URL.Query().Get("rootDir"))
	if nil != err {
		http.Error(w, "couldn't unescape rootDir. Error: "+err.Error(), 400)
		return
	}
	files := []*intelligentstore.FileDescriptor{}

	log.Printf("rootDir: '%s'\n", rootDir)

	type subDirInfoMap map[string]int64 // map[name]nestedFileCount
	dirnames := subDirInfoMap{}         // dirname[nested file count]
	for _, file := range allFiles {
		log.Printf("filepath: '%s'\n", file.RelativePath)
		if !strings.HasPrefix(string(file.RelativePath), rootDir) {
			// not in this root dir
			continue
		}

		relativeFilePath := intelligentstore.NewRelativePath(
			strings.TrimPrefix(
				string(file.RelativePath),
				rootDir))

		indexOfSlash := strings.Index(string(relativeFilePath), "/")
		if indexOfSlash != -1 {
			// file is inside a dir (not in the root folder)
			dirnames[string(relativeFilePath)[0:indexOfSlash]]++
		} else {
			// file is in the dir we're searching inside
			files = append(files, file)
		}
	}

	dirs := []*subDirInfo{}
	for dirname, nestedFileCount := range dirnames {
		dirs = append(dirs, &subDirInfo{dirname, nestedFileCount})
	}

	data := &revisionInfoWithFiles{revision.VersionTimestamp, files, dirs}

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
	s.openTransactionsMap[bucket.BucketName+"__"+strconv.FormatInt(int64(transaction.VersionTimestamp), 10)] = transaction

	requestBytes, err := ioutil.ReadAll(r.Body)
	if nil != err {
		http.Error(w, "couldn't read request body. Error: "+err.Error(), 400)
		return
	}

	var openTxRequest protofiles.OpenTxRequest
	err = proto.Unmarshal(requestBytes, &openTxRequest)
	if nil != err {
		http.Error(w, "couldn't unmarshal proto of request body. Error: "+err.Error(), 400)
		return
	}

	fileDetectorsForRequiredFiles := &protofiles.FileDescriptorProtoList{}
	var hashAlreadyExists bool

	for _, fileDescriptorProto := range openTxRequest.FileDescriptorList.FileDescriptors {
		fileDescriptor := protobufs.FileDescriptorProtoToFileDescriptor(fileDescriptorProto)

		hashAlreadyExists, err = transaction.AddAlreadyExistingHash(fileDescriptor)
		if nil != err {
			http.Error(w, fmt.Sprintf("error detecting if a hash for %s (%s) already exists", fileDescriptor.Hash, fileDescriptor.RelativePath), 500)
			return
		}

		// if a file for the hash already exists, the transaction adds it to the files in version and we don't need it from the client
		if !hashAlreadyExists {
			fileDetectorsForRequiredFiles.FileDescriptors = append(
				fileDetectorsForRequiredFiles.FileDescriptors,
				fileDescriptorProto)
		}
	}

	openTxReponse := &protofiles.OpenTxResponse{
		RevisionStr:        int64(transaction.VersionTimestamp),
		FileDescriptorList: fileDetectorsForRequiredFiles,
	}

	responseBytes, err := proto.Marshal(openTxReponse)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't marshall response for the files the server requires. Error: %s", err), 500)
		return
	}

	_, err = w.Write([]byte(responseBytes))
	if nil != err {
		log.Printf(
			"failed to send a response back to the client for files required to open transaction. Bucket: '%s', Revision: '%d'. Error: %s\n",
			bucketName,
			transaction.VersionTimestamp,
			err)
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

	var uploadedFile protofiles.FileProto
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
