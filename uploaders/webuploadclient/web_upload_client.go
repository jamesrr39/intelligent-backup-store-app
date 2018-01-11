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
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/domain"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	protofiles "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/protobufs/proto_files"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

// WebUploadClient represents an http client for uploading files to an IntelligentStore
type WebUploadClient struct {
	storeURL       string
	bucketName     string
	folderPath     string
	excludeMatcher *excludesmatcher.ExcludesMatcher
	fs             afero.Fs
	linkReader     uploaders.LinkReader
}

// NewWebUploadClient creates a new WebUploadClient
func NewWebUploadClient(
	storeURL,
	bucketName,
	folderPath string,
	excludeMatcher *excludesmatcher.ExcludesMatcher,
) *WebUploadClient {

	return &WebUploadClient{
		storeURL,
		bucketName,
		folderPath,
		excludeMatcher,
		afero.NewOsFs(),
		uploaders.OsFsLinkReader,
	}
}

// UploadToStore backs up a directory on the local machine to the bucket in the store in the WebUploadClient
func (c *WebUploadClient) UploadToStore() error {
	// FIXME abort if error

	fileInfosMap, err := uploaders.BuildFileInfosMap(c.fs, c.linkReader, c.folderPath, c.excludeMatcher)
	if nil != err {
		return err
	}

	revisionVersion, requiredRelativePaths, err := c.openTx(fileInfosMap.ToSlice())
	if nil != err {
		return err
	}

	log.Printf("== requiredRelativePaths: %s\n", requiredRelativePaths)

	var requiredRegularFileRelativePaths []domain.RelativePath
	var requiredSymlinkRelativePaths []domain.RelativePath

	for _, requiredRelativePath := range requiredRelativePaths {
		fileInfo := fileInfosMap[requiredRelativePath]
		switch fileInfo.Type {
		case domain.FileTypeRegular:
			requiredRegularFileRelativePaths = append(requiredRegularFileRelativePaths, requiredRelativePath)
		case domain.FileTypeSymlink:
			requiredSymlinkRelativePaths = append(requiredSymlinkRelativePaths, requiredRelativePath)
		default:
			return fmt.Errorf("unsupported file type: '%d' for fileInfo: '%v'", fileInfo.Type, fileInfo)
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

func (c *WebUploadClient) uploadSymlinks(revisionVersion domain.RevisionVersion, fileInfosMap uploaders.FileInfoMap, requiredRelativePaths []domain.RelativePath) error {
	uploadSymlinksRequest := &protofiles.UploadSymlinksRequest{}
	for _, requiredRelativePath := range requiredRelativePaths {
		fileInfo := fileInfosMap[requiredRelativePath]

		filePath := filepath.Join(c.folderPath, string(fileInfo.RelativePath))

		dest, err := c.linkReader(filePath)
		if nil != err {
			return fmt.Errorf("couldn't read link for %s. Error: %s", filePath, err)
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
		return err
	}

	url := fmt.Sprintf("%s/api/buckets/%s/upload/%d/symlinks", c.storeURL, c.bucketName, revisionVersion)
	client := http.Client{Timeout: time.Minute}
	resp, err := client.Post(
		url,
		"application/octet-stream",
		bytes.NewBuffer(uploadSymlinksRequestBytes))
	if nil != err {
		return fmt.Errorf("couln't POST to %s. Error: %s", url, err)
	}
	defer resp.Body.Close()

	if 200 != resp.StatusCode {
		errMessageBytes, err := ioutil.ReadAll(resp.Body)
		if nil != err {
			errMessageBytes = []byte(fmt.Sprintf("couldn't read response body. Error: %v", err))
		}
		return fmt.Errorf("expected 200 response code but got %d. Response body: '%s'", resp.StatusCode, errMessageBytes)
	}

	return nil
}

func (c *WebUploadClient) fetchRequiredHashes(revisionVersion domain.RevisionVersion, relativePathsWithHashes []*domain.RelativePathWithHash) ([]domain.Hash, error) {
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
		return nil, err
	}

	fetchRequiredHashesRequest := http.Client{Timeout: time.Minute}

	url := fmt.Sprintf("%s/api/buckets/%s/upload/%d/hashes", c.storeURL, c.bucketName, revisionVersion)
	resp, err := fetchRequiredHashesRequest.Post(
		url,
		"application/octet-stream",
		bytes.NewBuffer(fetchRequiredHashesRequestBytes))
	if nil != err {
		return nil, fmt.Errorf("couln't POST to %s. Error: %s", url, err)
	}
	defer resp.Body.Close()

	// read the response body now; we will need it whether the response was good or bad.
	respBytes, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return nil, fmt.Errorf("couldn't read hashes upload response body. Error: %s", err)
	}

	if 200 != resp.StatusCode {
		return nil, fmt.Errorf("hashes upload to %s failed with error: HTTP %d: %s", url, resp.StatusCode, respBytes)
	}

	var getRequiredHashesResponse protofiles.GetRequiredHashesResponse
	err = proto.Unmarshal(respBytes, &getRequiredHashesResponse)
	if nil != err {
		return nil, fmt.Errorf("couldn't unmarshal hashes upload response body. Error: %s", err)
	}

	var hashes []domain.Hash
	for _, hash := range getRequiredHashesResponse.GetHashes() {
		hashes = append(hashes, domain.Hash(hash))
	}

	return hashes, nil
}

// openTx opens a transaction with the server and sends a list of files it wants to back up
func (c *WebUploadClient) openTx(fileInfos []*domain.FileInfo) (domain.RevisionVersion, []domain.RelativePath, error) {
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
		return 0, nil, errors.Wrap(err, "couldn't unmarshall the open transaction request response")
	}

	openTxClient := http.Client{Timeout: time.Second * 20}

	openTxURL := c.storeURL + "/api/buckets/" + c.bucketName + "/upload"
	resp, err := openTxClient.Post(
		openTxURL,
		"application/octet-stream",
		bytes.NewBuffer(openTxRequestBodyBytes))
	if nil != err {
		return 0, nil, fmt.Errorf("couln't POST to %s. Error: %s", openTxURL, err)
	}
	defer resp.Body.Close()

	// read the response body now; we will need it whether the response was good or bad.
	respBytes, respReadErr := ioutil.ReadAll(resp.Body)

	if 200 != resp.StatusCode {
		var respErrMessage string
		if nil == respReadErr {
			respErrMessage = string(respBytes)
		} else {
			respErrMessage = fmt.Sprintf("couldn't read response error message. Error: %s", respReadErr)
		}

		return 0, nil, fmt.Errorf(
			"expected 200 (OK) repsonse code for open transaction, but received '%s' on POSTing to %s. Message body: %s",
			resp.Status, openTxURL, respErrMessage)
	}

	if nil != respReadErr {
		return 0, nil, fmt.Errorf("couldn't read OpenTx response. Error: %s", err)
	}

	var openTxResponse protofiles.OpenTxResponse
	err = proto.Unmarshal(respBytes, &openTxResponse)
	if nil != err {
		return 0, nil, fmt.Errorf("couldn't unmarshal OpenTx response. Error: %s", err)
	}

	var requiredRelativePaths []domain.RelativePath
	for _, wantedHash := range openTxResponse.GetRequiredRelativePaths() {
		requiredRelativePaths = append(requiredRelativePaths, domain.NewRelativePath(wantedHash))
	}
	log.Printf("created a new version: %d\n", openTxResponse.GetRevisionID())
	return domain.RevisionVersion(openTxResponse.GetRevisionID()), requiredRelativePaths, nil
}

func (c *WebUploadClient) backupFile(revisionStr domain.RevisionVersion, relativePath domain.RelativePath) error {
	log.Printf("BACKING UP %s\n", relativePath)

	client := http.Client{Timeout: time.Hour}
	fileContents, err := afero.ReadFile(c.fs, filepath.Join(
		c.folderPath,
		string(relativePath)))
	if nil != err {
		return errors.Wrapf(err, "couldn't read file at %s", relativePath)
	}

	protoBufFile := &protofiles.FileContentsProto{
		Contents: fileContents,
	}

	marshalledFile, err := proto.Marshal(protoBufFile)
	if nil != err {
		return errors.Wrapf(err, "couldn't marshall file at %s to protobuf", relativePath)
	}

	uploadURL := fmt.Sprintf("%s/api/buckets/%s/upload/%d/file",
		c.storeURL, c.bucketName, revisionStr)

	log.Printf("UPLOADING file to %s\n", uploadURL)

	resp, err := client.Post(uploadURL, "application/octet-stream", bytes.NewBuffer(marshalledFile))
	if nil != err {
		return errors.Wrapf(err, "couldn't send file at %s to remote Store server", relativePath)
	}
	defer resp.Body.Close()

	if 200 != resp.StatusCode {
		respBodyBytes, err := ioutil.ReadAll(resp.Body)
		if nil != err {
			respBodyBytes = []byte(fmt.Sprintf("couldn't read response body. Error: '%s'", err))
		}
		return fmt.Errorf("expected 200 (OK) repsonse code for file upload for '%s' to '%s', but received '%s'. Response Text: '%s'",
			string(relativePath),
			uploadURL,
			resp.Status,
			respBodyBytes)
	}
	return nil
}

func (c *WebUploadClient) commitTx(revisionStr domain.RevisionVersion) error {
	commitTxClient := http.Client{Timeout: time.Second * 20}
	url := fmt.Sprintf("%s/api/buckets/%s/upload/%d/commit", c.storeURL, c.bucketName, revisionStr)
	resp, err := commitTxClient.Get(url)
	if nil != err {
		return errors.Wrap(err, "couldn't commit upload transaction")
	}
	defer resp.Body.Close()

	if 200 != resp.StatusCode {
		var errorMessageBodyByteString string
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if nil != err {
			errorMessageBodyByteString = fmt.Sprintf(
				"couldn't read response body. Error: %s", err)
		} else {
			errorMessageBodyByteString = fmt.Sprintf(
				"response text: '%s'", string(bodyBytes))
		}
		return fmt.Errorf(
			"expected 200 (OK) repsonse code for commit, but received '%s'. Response Text: '%s'",
			resp.Status,
			errorMessageBodyByteString)
	}

	return nil
}
