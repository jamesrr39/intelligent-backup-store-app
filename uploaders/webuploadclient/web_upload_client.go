package webuploadclient

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/excludesmatcher"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/goutil/httpextra"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	protofiles "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/protobufs/proto_files"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders"
)

// WebUploadClient represents an http client for uploading files to an IntelligentStore
type WebUploadClient struct {
	storeURL       string
	bucketName     string
	folderPath     string
	excludeMatcher *excludesmatcher.ExcludesMatcher
	fs             gofs.Fs
	backupDryRun   bool
}

// NewWebUploadClient creates a new WebUploadClient
func NewWebUploadClient(
	storeURL,
	bucketName,
	folderPath string,
	excludeMatcher *excludesmatcher.ExcludesMatcher,
	backupDryRun bool,
) *WebUploadClient {

	return &WebUploadClient{
		storeURL,
		bucketName,
		folderPath,
		excludeMatcher,
		gofs.NewOsFs(),
		backupDryRun,
	}
}

// UploadToStore backs up a directory on the local machine to the bucket in the store in the WebUploadClient
func (c *WebUploadClient) UploadToStore() errorsx.Error {
	fileInfosMap, err := uploaders.BuildFileInfosMap(c.fs, c.folderPath, c.excludeMatcher)
	if nil != err {
		return err
	}

	revisionVersion, requiredRelativePaths, err := c.openTx(fileInfosMap.ToSlice())
	if nil != err {
		return err
	}

	log.Printf("== requiredRelativePaths: %s\n", requiredRelativePaths)

	var requiredRegularFileRelativePaths []intelligentstore.RelativePath
	var requiredSymlinkRelativePaths []intelligentstore.RelativePath

	for _, requiredRelativePath := range requiredRelativePaths {
		fileInfo := fileInfosMap[requiredRelativePath]
		switch fileInfo.Type {
		case intelligentstore.FileTypeRegular:
			requiredRegularFileRelativePaths = append(requiredRegularFileRelativePaths, requiredRelativePath)
		case intelligentstore.FileTypeSymlink:
			requiredSymlinkRelativePaths = append(requiredSymlinkRelativePaths, requiredRelativePath)
		default:
			return errorsx.Errorf("unsupported file type: '%d' for fileInfo: '%v'", fileInfo.Type, fileInfo)
		}
	}

	err = c.uploadSymlinks(revisionVersion, fileInfosMap, requiredSymlinkRelativePaths)
	if nil != err {
		return err
	}

	hashRelativePathMap, err := uploaders.BuildRelativePathsWithHashes(c.fs, c.folderPath, requiredRegularFileRelativePaths)
	if nil != err {
		return err
	}

	requiredHashes, err := c.fetchRequiredHashes(revisionVersion, hashRelativePathMap.ToSlice())
	if nil != err {
		return err
	}

	if c.backupDryRun {
		return nil
	}

	for _, requiredHash := range requiredHashes {
		relativePath := hashRelativePathMap[requiredHash][0]
		err = c.backupFile(revisionVersion, relativePath)
		if nil != err {
			return err
		}
	}

	err = c.commitTx(revisionVersion)
	if nil != err {
		return err
	}

	return nil
}

func (c *WebUploadClient) uploadSymlinks(revisionVersion intelligentstore.RevisionVersion, fileInfosMap uploaders.FileInfoMap, requiredRelativePaths []intelligentstore.RelativePath) errorsx.Error {
	uploadSymlinksRequest := &protofiles.UploadSymlinksRequest{}
	for _, requiredRelativePath := range requiredRelativePaths {
		fileInfo := fileInfosMap[requiredRelativePath]

		filePath := filepath.Join(c.folderPath, string(fileInfo.RelativePath))

		dest, err := c.fs.Readlink(filePath)
		if nil != err {
			return errorsx.Wrap(err, "filePath", filePath)
		}

		uploadSymlinksRequest.SymlinksWithRelativePaths = append(
			uploadSymlinksRequest.SymlinksWithRelativePaths,
			&protofiles.SymlinkWithRelativePath{
				RelativePath: string(fileInfo.RelativePath),
				Dest:         dest,
			},
		)
	}

	uploadSymlinksRequestBytes, err := proto.Marshal(uploadSymlinksRequest)
	if nil != err {
		return errorsx.Wrap(err)
	}

	url := fmt.Sprintf("%s/api/buckets/%s/upload/%d/symlinks", c.storeURL, c.bucketName, revisionVersion)
	client := http.Client{Timeout: time.Minute}
	resp, err := client.Post(
		url,
		"application/octet-stream",
		bytes.NewBuffer(uploadSymlinksRequestBytes))
	if nil != err {
		return errorsx.Wrap(err, "url", url)
	}
	defer resp.Body.Close()

	err = httpextra.CheckResponseCode(http.StatusOK, resp.StatusCode)
	if err != nil {
		return errorsx.Wrap(err, "body", httpextra.GetBodyOrErrorMsg(resp))
	}

	return nil
}

func (c *WebUploadClient) fetchRequiredHashes(revisionVersion intelligentstore.RevisionVersion, relativePathsWithHashes []*intelligentstore.RelativePathWithHash) ([]intelligentstore.Hash, errorsx.Error) {
	fetchRequiredHashesRequestProto := &protofiles.GetRequiredHashesRequest{
		RelativePathsAndHashes: nil,
	}

	for _, relativePathWithHash := range relativePathsWithHashes {
		fetchRequiredHashesRequestProto.RelativePathsAndHashes = append(
			fetchRequiredHashesRequestProto.RelativePathsAndHashes,
			&protofiles.RelativePathAndHashProto{
				RelativePath: string(relativePathWithHash.RelativePath),
				Hash:         string(relativePathWithHash.Hash),
			},
		)
	}

	fetchRequiredHashesRequestBytes, err := proto.Marshal(fetchRequiredHashesRequestProto)
	if nil != err {
		return nil, errorsx.Wrap(err)
	}

	fetchRequiredHashesRequest := http.Client{Timeout: time.Minute}

	url := fmt.Sprintf("%s/api/buckets/%s/upload/%d/hashes", c.storeURL, c.bucketName, revisionVersion)
	resp, err := fetchRequiredHashesRequest.Post(
		url,
		"application/octet-stream",
		bytes.NewBuffer(fetchRequiredHashesRequestBytes))
	if nil != err {
		return nil, errorsx.Wrap(err, "url", url)
	}
	defer resp.Body.Close()

	err = httpextra.CheckResponseCode(http.StatusOK, resp.StatusCode)
	if err != nil {
		return nil, errorsx.Wrap(err, "body", httpextra.GetBodyOrErrorMsg(resp))
	}

	// read the response body now; we will need it whether the response was good or bad.
	respBytes, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return nil, errorsx.Wrap(err, "detail", "couldn't read hashes upload response body")
	}

	var getRequiredHashesResponse protofiles.GetRequiredHashesResponse
	err = proto.Unmarshal(respBytes, &getRequiredHashesResponse)
	if nil != err {
		return nil, errorsx.Wrap(err, "detail", "couldn't unmarshal hashes upload response body")
	}

	var hashes []intelligentstore.Hash
	for _, hash := range getRequiredHashesResponse.GetHashes() {
		hashes = append(hashes, intelligentstore.Hash(hash))
	}

	return hashes, nil
}

// openTx opens a transaction with the server and sends a list of files it wants to back up
func (c *WebUploadClient) openTx(fileInfos []*intelligentstore.FileInfo) (intelligentstore.RevisionVersion, []intelligentstore.RelativePath, errorsx.Error) {
	openTxRequest := &protofiles.OpenTxRequest{
		FileInfos: nil,
	}

	for _, fileInfo := range fileInfos {
		openTxRequest.FileInfos = append(
			openTxRequest.FileInfos,
			&protofiles.FileInfoProto{
				RelativePath: string(fileInfo.RelativePath),
				ModTime:      fileInfo.ModTime.Unix(),
				Size:         fileInfo.Size,
				FileType:     protofiles.FileType(fileInfo.Type),
			},
		)
	}

	openTxRequestBodyBytes, err := proto.Marshal(openTxRequest)
	if nil != err {
		return 0, nil, errorsx.Wrap(err, "detail", "couldn't unmarshall the open transaction request response")
	}

	openTxClient := http.Client{Timeout: time.Second * 20}

	openTxURL := c.storeURL + "/api/buckets/" + c.bucketName + "/upload"
	resp, err := openTxClient.Post(
		openTxURL,
		"application/octet-stream",
		bytes.NewBuffer(openTxRequestBodyBytes))
	if nil != err {
		return 0, nil, errorsx.Wrap(err, "openTxURL", openTxURL)
	}
	defer resp.Body.Close()

	err = httpextra.CheckResponseCode(http.StatusOK, resp.StatusCode)
	if err != nil {
		return 0, nil, errorsx.Wrap(err, "body", httpextra.GetBodyOrErrorMsg(resp))
	}

	// read the response body now; we will need it whether the response was good or bad.
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, errorsx.Wrap(err)
	}

	var openTxResponse protofiles.OpenTxResponse
	err = proto.Unmarshal(respBytes, &openTxResponse)
	if nil != err {
		return 0, nil, errorsx.Wrap(err)
	}

	var requiredRelativePaths []intelligentstore.RelativePath
	for _, wantedHash := range openTxResponse.GetRequiredRelativePaths() {
		requiredRelativePaths = append(requiredRelativePaths, intelligentstore.NewRelativePath(wantedHash))
	}
	log.Printf("created a new version: %d\n", openTxResponse.GetRevisionID())
	return intelligentstore.RevisionVersion(openTxResponse.GetRevisionID()), requiredRelativePaths, nil
}

func (c *WebUploadClient) backupFile(revisionStr intelligentstore.RevisionVersion, relativePath intelligentstore.RelativePath) errorsx.Error {
	log.Printf("BACKING UP %s\n", relativePath)

	client := http.Client{Timeout: time.Hour}
	fileContents, err := c.fs.ReadFile(filepath.Join(
		c.folderPath,
		string(relativePath)))
	if nil != err {
		return errorsx.Wrap(err, "relativePath", relativePath)
	}

	protoBufFile := &protofiles.FileContentsProto{
		Contents: fileContents,
	}

	marshalledFile, err := proto.Marshal(protoBufFile)
	if nil != err {
		return errorsx.Wrap(err, "relativePath", relativePath)
	}

	uploadURL := fmt.Sprintf("%s/api/buckets/%s/upload/%d/file",
		c.storeURL, c.bucketName, revisionStr)

	log.Printf("UPLOADING file to %s\n", uploadURL)

	resp, err := client.Post(uploadURL, "application/octet-stream", bytes.NewBuffer(marshalledFile))
	if nil != err {
		return errorsx.Wrap(err, "relativePath", relativePath)
	}
	defer resp.Body.Close()

	err = httpextra.CheckResponseCode(http.StatusOK, resp.StatusCode)
	if err != nil {
		return errorsx.Wrap(err, "body", httpextra.GetBodyOrErrorMsg(resp))
	}

	return nil
}

func (c *WebUploadClient) commitTx(revisionStr intelligentstore.RevisionVersion) errorsx.Error {
	commitTxClient := http.Client{Timeout: time.Second * 20}
	url := fmt.Sprintf("%s/api/buckets/%s/upload/%d/commit", c.storeURL, c.bucketName, revisionStr)
	resp, err := commitTxClient.Get(url)
	if nil != err {
		return errorsx.Wrap(err, "couldn't commit upload transaction")
	}
	defer resp.Body.Close()

	err = httpextra.CheckResponseCode(http.StatusOK, resp.StatusCode)
	if err != nil {
		return errorsx.Wrap(err, "body", httpextra.GetBodyOrErrorMsg(resp))
	}

	return nil
}
