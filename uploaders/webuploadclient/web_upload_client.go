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
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
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
}

// NewWebUploadClient creates a new WebUploadClient
func NewWebUploadClient(
	storeURL, bucketName, folderPath string,
	excludeMatcher *excludesmatcher.ExcludesMatcher,
) *WebUploadClient {

	return &WebUploadClient{
		storeURL,
		bucketName,
		folderPath,
		excludeMatcher,
		afero.NewOsFs(),
	}
}

// UploadToStore backs up a directory on the local machine to the bucket in the store in the WebUploadClient
func (c *WebUploadClient) UploadToStore() error {
	// FIXME abort if error

	fileInfosMap, err := uploaders.BuildFileInfosMap(c.fs, c.folderPath, c.excludeMatcher)
	if nil != err {
		return err
	}

	revisionVersion, requiredRelativePaths, err := c.openTx(fileInfosMap.ToSlice())
	if nil != err {
		return err
	}

	hashRelativePathMap, err := uploaders.BuildRelativePathsWithHashes(c.fs, c.folderPath, requiredRelativePaths)
	if nil != err {
		return err
	}

	log.Printf("paths:%v\n", hashRelativePathMap.ToSlice())

	requiredHashes, err := c.fetchRequiredHashes(revisionVersion, hashRelativePathMap.ToSlice())
	if nil != err {
		return err
	}
	log.Printf("hashes left: %v\n", requiredHashes)

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

func (c *WebUploadClient) fetchRequiredHashes(revisionVersion intelligentstore.RevisionVersion, relativePathsWithHashes []*intelligentstore.RelativePathWithHash) ([]intelligentstore.Hash, error) {
	fetchRequiredHashesRequestProto := &protofiles.GetRequiredHashesRequest{
		RelativePathAndHash: nil,
	}

	for _, relativePathWithHash := range relativePathsWithHashes {
		fetchRequiredHashesRequestProto.RelativePathAndHash = append(
			fetchRequiredHashesRequestProto.RelativePathAndHash,
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

	fetchRequiredHashesRequest := http.Client{Timeout: time.Second * 20}

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

	log.Printf("h proto: %v\n", getRequiredHashesResponse.GetHashes())
	var hashes []intelligentstore.Hash
	for _, hash := range getRequiredHashesResponse.GetHashes() {
		hashes = append(hashes, intelligentstore.Hash(hash))
	}

	return hashes, nil
}

/*
	// open transaction
	revisionID, wantedHashes, err := c.openTx(fileList)
	if nil != err {
		return err
	}
	log.Printf("opened Tx. Rev: %d\n", revisionID)

	var filesSuccessfullyBackedUpCount int64
	var filesFailedToBackup []*intelligentstore.FileDescriptor
	amountOfFilesToSend := len(wantedHashes)
	filesAlreadyOnServerCount := totalFilesToUpload - int64(amountOfFilesToSend)

	for _, wantedHash := range wantedHashes {
		fileDescriptor := hashDescriptorMap[wantedHash][0]
		err = c.backupFile(revisionID, fileDescriptor)
		if nil != err {
			filesFailedToBackup = append(filesFailedToBackup, fileDescriptor)
			log.Printf("failed to backup %s. Error: %s\n", fileDescriptor, err)
		} else {
			filesSuccessfullyBackedUpCount++
		}

		filesProcessedSoFar := filesSuccessfullyBackedUpCount + int64(len(filesFailedToBackup))
		if (filesProcessedSoFar % 10) == 0 {
			log.Printf("%d of %d files processed (%f%% complete) (%d were already on the server)\n",
				filesProcessedSoFar,
				amountOfFilesToSend,
				100*float64(filesProcessedSoFar)/float64(amountOfFilesToSend),
				filesAlreadyOnServerCount)
		}
	}

	log.Println("commiting Tx")
	err = c.commitTx(revisionID)
	if nil != err {
		return err
	}

	return nil
}
*/
// openTx opens a transaction with the server and sends a list of files it wants to back up
func (c *WebUploadClient) openTx(fileInfos []*intelligentstore.FileInfo) (intelligentstore.RevisionVersion, []intelligentstore.RelativePath, error) {
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

	var requiredRelativePaths []intelligentstore.RelativePath
	for _, wantedHash := range openTxResponse.GetRequiredRelativePaths() {
		requiredRelativePaths = append(requiredRelativePaths, intelligentstore.NewRelativePath(wantedHash))
	}
	log.Printf("created a new version: %d\n", openTxResponse.GetRevisionID())
	return intelligentstore.RevisionVersion(openTxResponse.GetRevisionID()), requiredRelativePaths, nil
}

func (c *WebUploadClient) backupFile(revisionStr intelligentstore.RevisionVersion, relativePath intelligentstore.RelativePath) error {
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

func (c *WebUploadClient) commitTx(revisionStr intelligentstore.RevisionVersion) error {
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
