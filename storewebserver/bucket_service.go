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
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/golang/protobuf/proto"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/logpkg"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	protofiles "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/protobufs/proto_files"
)

// BucketService handles HTTP requests to get bucket information.
type BucketService struct {
	logger *logpkg.Logger
	store  *dal.IntelligentStoreDAL
	http.Handler
	openTransactionsMap
}

type openTransactionsMap map[string]*intelligentstore.Transaction

type subDirInfo struct {
	Name string `json:"name"`
}

// NewBucketService creates a new BucketService and a router for handling requests.
func NewBucketService(logger *logpkg.Logger, store *dal.IntelligentStoreDAL) *BucketService {
	router := chi.NewRouter()
	bucketService := &BucketService{logger, store, router, make(openTransactionsMap)}

	// swagger:route GET /api/buckets/ bucket listBuckets
	//     Produces:
	//     - application/json
	router.Get("/", bucketService.handleGetAllBuckets)

	// swagger:route GET /api/buckets/{bucketName} bucket getBucket
	//     Produces:
	//     - application/json
	router.Get("/{bucketName}", bucketService.handleGetBucket)
	router.Post("/{bucketName}/upload", bucketService.handleCreateRevision)
	router.Post("/{bucketName}/upload/{revisionTs}/symlinks", bucketService.handleUploadSymlinks)
	router.Post("/{bucketName}/upload/{revisionTs}/hashes", bucketService.handleUploadHashes)
	router.Post("/{bucketName}/upload/{revisionTs}/file", bucketService.handleUploadFile)
	router.Get("/{bucketName}/upload/{revisionTs}/commit", bucketService.handleCommitTransaction)

	router.Get("/{bucketName}/{revisionTs}", bucketService.handleGetRevision)
	router.Get("/{bucketName}/{revisionTs}/file", bucketService.handleGetFileContents)
	return bucketService
}

type bucketSummary struct {
	Name           string                            `json:"name"`
	LastRevisionTs *intelligentstore.RevisionVersion `json:"lastRevisionTs"`
}

type revisionInfoWithFiles struct {
	LastRevisionTs intelligentstore.RevisionVersion  `json:"revisionTs"`
	Files          []intelligentstore.FileDescriptor `json:"files"`
	Dirs           []*subDirInfo                     `json:"dirs"`
}

// @Title Get Latest Buckets Information
// @Success 200 {object} string &quot;Success&quot;
func (s *BucketService) handleGetAllBuckets(w http.ResponseWriter, r *http.Request) {
	var err error

	buckets, err := s.store.BucketDAL.GetAllBuckets()
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if len(buckets) == 0 {
		w.Write([]byte("[]"))
		return
	}

	var bucketsSummaries []*bucketSummary
	var latestRevision *intelligentstore.Revision
	var latestRevisionTs *intelligentstore.RevisionVersion
	for _, bucket := range buckets {
		latestRevision, err = s.store.RevisionDAL.GetLatestRevision(bucket)
		if nil != err {
			if errorsx.Cause(err) != dal.ErrNoRevisionsForBucket {
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
	bucketName := chi.URLParam(r, "bucketName")

	bucket, err := s.store.BucketDAL.GetBucketByName(bucketName)
	if nil != err {
		if dal.ErrBucketDoesNotExist == err {
			http.Error(w, err.Error(), 404)
			return
		}
		http.Error(w, err.Error(), 500)
		return
	}

	revisions, err := s.store.RevisionDAL.GetRevisions(bucket)
	if nil != err {
		http.Error(w, err.Error(), 500)
		return
	}

	if len(revisions) == 0 {
		revisions = make([]*intelligentstore.Revision, 0)
	}

	sort.Slice(revisions, func(i int, j int) bool {
		return revisions[i].VersionTimestamp < revisions[j].VersionTimestamp
	})

	render.JSON(w, r, handleGetBucketResponse{revisions})
}

func (s *BucketService) getRevision(bucketName, revisionTsString string) (*intelligentstore.Revision, *HTTPError) {
	var err error

	bucket, err := s.store.BucketDAL.GetBucketByName(bucketName)
	if nil != err {
		if dal.ErrBucketDoesNotExist == err {
			return nil, NewHTTPError(fmt.Errorf("couldn't find bucket '%s'. Error: %s", bucketName, err), 404)
		}
		return nil, NewHTTPError(err, 500)
	}

	var revision *intelligentstore.Revision
	if revisionTsString == "latest" {
		revision, err = s.store.RevisionDAL.GetLatestRevision(bucket)
		if dal.ErrNoRevisionsForBucket == err {
			return nil, NewHTTPError(err, 404)
		}
	} else {
		var revisionTimestamp int64
		revisionTimestamp, err = strconv.ParseInt(revisionTsString, 10, 64)
		if nil != err {
			return nil, NewHTTPError(fmt.Errorf("couldn't convert '%s' to a timestamp. Error: '%s'", revisionTsString, err), 400)
		}
		revision, err = s.store.RevisionDAL.GetRevision(bucket, intelligentstore.RevisionVersion(revisionTimestamp))
	}
	if nil != err {
		if err == dal.ErrRevisionDoesNotExist {
			return nil, NewHTTPError(err, 404)
		}

		return nil, NewHTTPError(err, 500)
	}
	return revision, nil
}

func (s *BucketService) handleGetRevision(w http.ResponseWriter, r *http.Request) {
	bucketName := chi.URLParam(r, "bucketName")
	revisionTsString := chi.URLParam(r, "revisionTs")

	revision, revErr := s.getRevision(bucketName, revisionTsString)
	if nil != revErr {
		http.Error(w, revErr.Error(), revErr.StatusCode)
		return
	}

	rootDir := r.URL.Query().Get("rootDir")

	dirEntries, err := s.store.RevisionDAL.ReadDir(revision.Bucket, revision, intelligentstore.NewRelativePath(rootDir))
	if err != nil {
		errorsx.HTTPError(w, s.logger, errorsx.Wrap(err), http.StatusInternalServerError)
		return
	}

	data := revisionInfoWithFiles{
		LastRevisionTs: revision.VersionTimestamp,
		Files:          []intelligentstore.FileDescriptor{},
		Dirs:           []*subDirInfo{},
	}

	for _, descriptor := range dirEntries {
		switch descriptor.GetFileInfo().Type {
		case intelligentstore.FileTypeDir:
			data.Dirs = append(data.Dirs, &subDirInfo{Name: descriptor.GetFileInfo().RelativePath.Name()})
		case intelligentstore.FileTypeRegular, intelligentstore.FileTypeSymlink:
			data.Files = append(data.Files, descriptor)
		default:
			http.Error(w, fmt.Sprintf("unhandled file type: %q", descriptor.GetFileInfo().Type), http.StatusInternalServerError)
			return
		}
	}

	render.JSON(w, r, data)
}

func (s *BucketService) handleCreateRevision(w http.ResponseWriter, r *http.Request) {
	var err error

	bucketName := chi.URLParam(r, "bucketName")

	bucket, err := s.store.BucketDAL.GetBucketByName(bucketName)
	if nil != err {
		if dal.ErrBucketDoesNotExist == err {
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

	transaction, err := s.store.TransactionDAL.CreateTransaction(bucket, fileInfos)
	if nil != err {
		http.Error(w, "couldn't start a transaction. Error: "+err.Error(), 500)
		return
	}

	transactionString := fmt.Sprintf("%s__%d", bucket.BucketName, transaction.Revision.VersionTimestamp)
	s.openTransactionsMap[transactionString] = transaction

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
	var err error

	bucketName := chi.URLParam(r, "bucketName")
	revisionTsString := chi.URLParam(r, "revisionTs")

	bucket, err := s.store.BucketDAL.GetBucketByName(bucketName)
	if nil != err {
		if dal.ErrBucketDoesNotExist == err {
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

	var uploadedFile protofiles.FileContentsProto
	err = proto.Unmarshal(bodyBytes, &uploadedFile)
	if nil != err {
		http.Error(w, fmt.Sprintf("couldn't unmarshal message. Error: '%s'", err), 400)
		return
	}

	err = s.store.TransactionDAL.BackupFile(transaction,
		bytes.NewReader(uploadedFile.Contents))
	if nil != err {
		errCode := 500
		if errorsx.Cause(err) == dal.ErrFileNotRequiredForTransaction || errorsx.Cause(err) == dal.ErrFileAlreadyUploaded {
			errCode = 400
		}
		http.Error(w, err.Error(), errCode)
		return
	}
}

func (s *BucketService) handleCommitTransaction(w http.ResponseWriter, r *http.Request) {
	bucketName := chi.URLParam(r, "bucketName")
	revisionTsString := chi.URLParam(r, "revisionTs")

	bucket, err := s.store.BucketDAL.GetBucketByName(bucketName)
	if nil != err {
		if dal.ErrBucketDoesNotExist == err {
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

	err = s.store.TransactionDAL.Commit(transaction)
	if nil != err {
		http.Error(w, "failed to commit transaction. Error: "+err.Error(), 500)
		return
	}
	s.openTransactionsMap[bucketName+"__"+revisionTsString] = nil
}

func (s *BucketService) handleGetFileContents(w http.ResponseWriter, r *http.Request) {
	var err error

	bucketName := chi.URLParam(r, "bucketName")
	revisionTsString := chi.URLParam(r, "revisionTs")

	relativePath := intelligentstore.NewRelativePath(r.URL.Query().Get("relativePath"))
	revision, revErr := s.getRevision(bucketName, revisionTsString)
	if nil != revErr {
		http.Error(w, revErr.Error(), revErr.StatusCode)
		return
	}

	file, err := s.store.RevisionDAL.GetFileContentsInRevision(revision.Bucket, revision, relativePath)
	if nil != err {
		if err == dal.ErrNoFileWithThisRelativePathInRevision {
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
	var err error

	bucketName := chi.URLParam(r, "bucketName")
	revisionTsString := chi.URLParam(r, "revisionTs")

	bucket, err := s.store.BucketDAL.GetBucketByName(bucketName)
	if nil != err {
		if dal.ErrBucketDoesNotExist == err {
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
	var err error

	bucketName := chi.URLParam(r, "bucketName")
	revisionTsString := chi.URLParam(r, "revisionTs")

	bucket, err := s.store.BucketDAL.GetBucketByName(bucketName)
	if nil != err {
		if dal.ErrBucketDoesNotExist == err {
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
