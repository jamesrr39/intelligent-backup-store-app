package storewebserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
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
	router.HandleFunc("/{bucketName}/upload/{revisionTs}/symlinks", bucketService.handleUploadSymlinks).Methods("POST")
	router.HandleFunc("/{bucketName}/upload/{revisionTs}/hashes", bucketService.handleUploadHashes).Methods("POST")
	router.HandleFunc("/{bucketName}/upload/{revisionTs}/file", bucketService.handleUploadFile).Methods("POST")
	router.HandleFunc("/{bucketName}/upload/{revisionTs}/commit", bucketService.handleCommitTransaction).Methods("GET")

	router.HandleFunc("/{bucketName}/{revisionTs}", bucketService.handleGetRevision).Methods("GET")
	router.HandleFunc("/{bucketName}/{revisionTs}/file", bucketService.handleGetFileContents).Methods("GET")
	return bucketService
}

type bucketSummary struct {
	Name           string                            `json:"name"`
	LastRevisionTs *intelligentstore.RevisionVersion `json:"lastRevisionTs"`
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

		bucketsSummaries = append(bucketsSummaries, &bucketSummary{bucket.Name, latestRevisionTs})
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

	bucket, err := s.store.GetBucketByName(bucketName)
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

func (s *BucketService) getRevision(bucketName, revisionTsString string) (*intelligentstore.Revision, *HTTPError) {
	bucket, err := s.store.GetBucketByName(bucketName)
	if nil != err {
		if intelligentstore.ErrBucketDoesNotExist == err {
			return nil, NewHTTPError(fmt.Errorf("couldn't find bucket '%s'. Error: %s", bucketName, err), 404)
		}
		return nil, NewHTTPError(err, 500)
	}

	var revision *intelligentstore.Revision
	if "latest" == revisionTsString {
		revision, err = bucket.GetLatestRevision()
		if intelligentstore.ErrNoRevisionsForBucket == err {
			return nil, NewHTTPError(err, 404)
		}
	} else {
		var revisionTimestamp int64
		revisionTimestamp, err = strconv.ParseInt(revisionTsString, 10, 64)
		if nil != err {
			return nil, NewHTTPError(fmt.Errorf("couldn't convert '%s' to a timestamp. Error: '%s'", revisionTsString, err), 400)
		}
		revision, err = bucket.GetRevision(intelligentstore.RevisionVersion(revisionTimestamp))
	}
	if nil != err {
		if err == intelligentstore.ErrRevisionDoesNotExist {
			return nil, NewHTTPError(err, 404)
		}

		return nil, NewHTTPError(err, 500)
	}
	return revision, nil
}

func (s *BucketService) handleGetRevision(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	bucketName := vars["bucketName"]
	revisionTsString := vars["revisionTs"]

	revision, revErr := s.getRevision(bucketName, revisionTsString)
	if nil != revErr {
		http.Error(w, revErr.Error(), revErr.StatusCode)
		return
	}

	allFiles, err := revision.GetFilesInRevision()
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}

	rootDir := r.URL.Query().Get("rootDir")

	files := []intelligentstore.FileDescriptor{}

	type subDirInfoMap map[string]int64 // map[name]nestedFileCount
	dirnames := subDirInfoMap{}         // dirname[nested file count]
	for _, file := range allFiles {
		if !strings.HasPrefix(string(file.GetFileInfo().RelativePath), rootDir) {
			// not in this root dir
			continue
		}

		relativeFilePath := intelligentstore.NewRelativePath(
			strings.TrimPrefix(
				string(file.GetFileInfo().RelativePath),
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

	bucket, err := s.store.GetBucketByName(bucketName)
	if nil != err {
		if intelligentstore.ErrBucketDoesNotExist == err {
			http.Error(w, fmt.Sprintf("couldn't find bucket '%s'. Error: %s", bucketName, err), 404)
			return
		}
		http.Error(w, err.Error(), 500)
		return
	}

	requestBytes, err := ioutil.ReadAll(r.Body)
	if nil != err {
		http.Error(w, "couldn't read request body. Error: "+err.Error(), 400)
		return
	}

	openTxRequest := protofiles.OpenTxRequest{}
	err = proto.Unmarshal(requestBytes, &openTxRequest)
	if nil != err {
		http.Error(w, "couldn't unmarshal proto of request body. Error: "+err.Error(), 400)
		return
	}

	if nil == openTxRequest.GetFileInfos() {
		http.Error(w, "expected file info list but couldn't find one", 400)
		return
	}

	var fileInfos []*intelligentstore.FileInfo

	for _, fileInfoProto := range openTxRequest.GetFileInfos() {

		fileType, err := fileTypeProtoToFileType(fileInfoProto.GetFileType())
		if nil != err {
			http.Error(
				w,
				fmt.Sprintf(
					"couldn't parse file type '%s' for file info for '%s'",
					fileInfoProto.GetFileType().String(),
					fileInfoProto.GetRelativePath()),
				400)
			return
		}

		fileInfos = append(
			fileInfos,
			intelligentstore.NewFileInfo(
				fileType,
				intelligentstore.NewRelativePath(fileInfoProto.GetRelativePath()),
				time.Unix(fileInfoProto.GetModTime(), 0), // FIXME is this right?
				fileInfoProto.GetSize(),
				os.FileMode(fileInfoProto.GetMode()),
			),
		)
	}

	transaction, err := bucket.Begin(fileInfos)
	if nil != err {
		http.Error(w, "couldn't start a transaction. Error: "+err.Error(), 500)
		return
	}
	s.openTransactionsMap[bucket.Name+"__"+strconv.FormatInt(int64(transaction.Revision.VersionTimestamp), 10)] = transaction

	var relativePaths []string
	for _, relativePath := range transaction.GetRelativePathsRequired() {
		relativePaths = append(relativePaths, string(relativePath))
	}

	openTxReponse := &protofiles.OpenTxResponse{
		RevisionID:            int64(transaction.Revision.VersionTimestamp),
		RequiredRelativePaths: relativePaths,
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
			transaction.Revision.VersionTimestamp,
			err)
	}
}

func fileTypeProtoToFileType(protoFileType protofiles.FileType) (intelligentstore.FileType, error) {
	switch protoFileType {
	case protofiles.FileType_REGULAR:
		return intelligentstore.FileTypeRegular, nil
	case protofiles.FileType_SYMLINK:
		return intelligentstore.FileTypeSymlink, nil
	default:
		return intelligentstore.FileTypeUnknown, errors.New("didn't recognise proto file type: " + protoFileType.String())
	}
}

func (s *BucketService) handleUploadFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	bucketName := vars["bucketName"]
	revisionTsString := vars["revisionTs"]

	bucket, err := s.store.GetBucketByName(bucketName)
	if nil != err {
		if intelligentstore.ErrBucketDoesNotExist == err {
			http.Error(w, fmt.Sprintf("couldn't find bucket '%s'. Error: %s", bucketName, err), 404)
			return
		}
		http.Error(w, err.Error(), 500)
		return
	}

	transaction := s.openTransactionsMap[bucket.Name+"__"+revisionTsString]
	if nil == transaction {
		http.Error(w, fmt.Sprintf("there is no open transaction for bucket %s and revisionTs %s", bucket.Name, revisionTsString), 400)
		return
	}

	bodyBytes, err := ioutil.ReadAll(r.Body)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't read response body. Error: '%s'", err), 400)
		return
	}
	defer r.Body.Close()

	var uploadedFile protofiles.FileContentsProto
	err = proto.Unmarshal(bodyBytes, &uploadedFile)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't unmarshal message. Error: '%s'", err), 400)
		return
	}

	err = transaction.BackupFile(
		bytes.NewBuffer(uploadedFile.Contents))
	if nil != err {
		errCode := 500
		if intelligentstore.ErrFileNotRequiredForTransaction == err {
			errCode = 400
		}
		http.Error(w, err.Error(), errCode)
		return
	}
}

func (s *BucketService) handleCommitTransaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	bucketName := vars["bucketName"]
	revisionTsString := vars["revisionTs"]

	bucket, err := s.store.GetBucketByName(bucketName)
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
		http.Error(w, fmt.Sprintf("there is no open transaction for bucket %s and revisionTs %s", bucket.Name, revisionTsString), 400)
		return
	}

	err = transaction.Commit()
	if nil != err {
		http.Error(w, "failed to commit transaction. Error: "+err.Error(), 500)
		return
	}
	s.openTransactionsMap[bucketName+"__"+revisionTsString] = nil
}

func (s *BucketService) handleGetFileContents(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	bucketName := vars["bucketName"]
	revisionTsString := vars["revisionTs"]

	relativePath := intelligentstore.NewRelativePath(r.URL.Query().Get("relativePath"))
	revision, revErr := s.getRevision(bucketName, revisionTsString)
	if nil != revErr {
		http.Error(w, revErr.Error(), revErr.StatusCode)
		return
	}

	file, err := revision.GetFileContentsInRevision(relativePath)
	if nil != err {
		if err == intelligentstore.ErrNoFileWithThisRelativePathInRevision {
			http.Error(w, fmt.Sprintf("couldn't get '%s'", relativePath), 404)
			return
		}
		http.Error(w, fmt.Sprintf("error getting file: '%s'. Error: %s", relativePath, err), 500)
		return
	}
	defer file.Close()

	_, err = io.Copy(w, file)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't copy file. Error: %s", err), 500)
		return
	}
}

func (s *BucketService) handleUploadHashes(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	bucketName := vars["bucketName"]
	revisionTsString := vars["revisionTs"]

	bucket, err := s.store.GetBucketByName(bucketName)
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
		http.Error(w, fmt.Sprintf("there is no open transaction for bucket %s and revisionTs %s", bucket.Name, revisionTsString), 400)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't read request body. Error: %s", err.Error()), 400)
		return
	}

	var getRequiredHashesRequest protofiles.GetRequiredHashesRequest
	err = proto.Unmarshal(body, &getRequiredHashesRequest)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't unmarshall proto upload hashes. Error: %s", err.Error()), 500)
		return
	}

	var relativePathsWithHashes []*intelligentstore.RelativePathWithHash
	for _, relativePathAndHashProto := range getRequiredHashesRequest.GetRelativePathsAndHashes() {
		relativePathsWithHashes = append(relativePathsWithHashes, &intelligentstore.RelativePathWithHash{
			RelativePath: intelligentstore.NewRelativePath(relativePathAndHashProto.GetRelativePath()),
			Hash:         intelligentstore.Hash(relativePathAndHashProto.GetHash()),
		})
	}

	hashes, err := transaction.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't process upload hashes and get required uploads. Error: %s", err.Error()), 500)
		return
	}

	getRequiredHashesResponse := &protofiles.GetRequiredHashesResponse{
		Hashes: nil,
	}

	for _, hash := range hashes {
		getRequiredHashesResponse.Hashes = append(
			getRequiredHashesResponse.Hashes,
			string(hash),
		)
	}

	responseBytes, err := proto.Marshal(getRequiredHashesResponse)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't marshal get required uploads response. Error: %s", err.Error()), 500)
		return
	}

	w.Header().Set("content-type", "application/octet-stream")
	_, err = w.Write(responseBytes)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't write get required uploads response. Error: %s", err.Error()), 500)
		return
	}
}

func (s *BucketService) handleUploadSymlinks(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	bucketName := vars["bucketName"]
	revisionTsString := vars["revisionTs"]

	bucket, err := s.store.GetBucketByName(bucketName)
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
		http.Error(w, fmt.Sprintf("there is no open transaction for bucket %s and revisionTs %s", bucket.Name, revisionTsString), 400)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't read request body. Error: %s", err.Error()), 400)
		return
	}

	var uploadSymlinksRequest protofiles.UploadSymlinksRequest
	err = proto.Unmarshal(body, &uploadSymlinksRequest)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't unmarshal proto upload symlinks. Error: %s", err.Error()), 500)
		return
	}

	var symlinksWithRelativePaths []*intelligentstore.SymlinkWithRelativePath
	for _, symlinkWithRelativePath := range uploadSymlinksRequest.SymlinksWithRelativePaths {
		symlinksWithRelativePaths = append(
			symlinksWithRelativePaths,
			&intelligentstore.SymlinkWithRelativePath{
				RelativePath: intelligentstore.NewRelativePath(symlinkWithRelativePath.GetRelativePath()),
				Dest:         symlinkWithRelativePath.GetDest(),
			},
		)
	}

	err = transaction.ProcessSymlinks(symlinksWithRelativePaths)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't process upload symlinks. Error: %s", err.Error()), 500)
		return
	}
}
