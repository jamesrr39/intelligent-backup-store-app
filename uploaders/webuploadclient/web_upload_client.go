package webuploadclient

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/protobufs"
	protofiles "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/protobufs/proto_files"
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

// BackupFolder backs up a directory on the local machine to the bucket in the store in the WebUploadClient
func (c *WebUploadClient) UploadToStore() error {
	var fileList []*intelligentstore.FileDescriptor

	var totalFilesToUpload int64
	var totalBytesToUpload int64

	// build file list
	err := afero.Walk(c.fs, c.folderPath, func(path string, fileInfo os.FileInfo, err error) error {
		if nil != err {
			return err
		}

		if fileInfo.IsDir() {
			return nil
		}

		if !fileInfo.Mode().IsRegular() {
			// skip symlinks
			return nil
		}

		relativeFilePath := intelligentstore.NewRelativePath(strings.TrimPrefix(path, c.folderPath))

		if c.excludeMatcher.Matches(relativeFilePath) {
			// skip excluded file
			log.Printf("skipping %s\n", relativeFilePath)
			return nil
		}

		log.Printf("adding %s to the file descriptor list\n", relativeFilePath)

		totalFilesToUpload++
		totalBytesToUpload += fileInfo.Size()

		file, err := c.fs.Open(path)
		if nil != err {
			return err
		}
		defer file.Close()

		fileDescriptor, err := intelligentstore.NewFileDescriptorFromReader(relativeFilePath, file)
		if nil != err {
			return err
		}

		fileList = append(fileList, fileDescriptor)

		return nil
	})
	if nil != err {
		return err
	}

	// open transaction
	revisionStr, filesToSendDescriptors, err := c.openTx(fileList)
	if nil != err {
		return err
	}
	log.Printf("opened Tx. Rev: %d\n", revisionStr)

	var filesSuccessfullyBackedUpCount int64
	var filesFailedToBackup []*intelligentstore.FileDescriptor
	amountOfFilesToSend := len(filesToSendDescriptors)
	filesAlreadyOnServerCount := totalFilesToUpload - int64(amountOfFilesToSend)

	for _, fileDescriptor := range filesToSendDescriptors {
		err = c.backupFile(revisionStr, fileDescriptor)
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
	err = c.commitTx(revisionStr)
	if nil != err {
		return err
	}

	return nil
}

// openTx opens a transaction with the server and sends a list of files it wants to back up
func (c *WebUploadClient) openTx(fileDescriptors []*intelligentstore.FileDescriptor) (intelligentstore.RevisionVersion, []*intelligentstore.FileDescriptor, error) {
	protoFileDescriptors := &protofiles.FileDescriptorProtoList{}
	for _, descriptor := range fileDescriptors {
		descriptorProto := &protofiles.FileDescriptorProto{
			Filename: string(descriptor.RelativePath),
			Hash:     string(descriptor.Hash),
		}

		protoFileDescriptors.FileDescriptors = append(protoFileDescriptors.FileDescriptors, descriptorProto)
	}

	openTxRequest := &protofiles.OpenTxRequest{
		FileDescriptorList: protoFileDescriptors,
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

	var filesToSendDescriptors []*intelligentstore.FileDescriptor
	for _, fileDescriptorProto := range openTxResponse.FileDescriptorList.FileDescriptors {
		filesToSendDescriptors = append(
			filesToSendDescriptors,
			protobufs.FileDescriptorProtoToFileDescriptor(fileDescriptorProto))
	}
	log.Printf("created a new version: %d\n", openTxResponse.RevisionStr)
	return intelligentstore.RevisionVersion(openTxResponse.RevisionStr), filesToSendDescriptors, nil
}

func (c *WebUploadClient) backupFile(revisionStr intelligentstore.RevisionVersion, fileDescriptor *intelligentstore.FileDescriptor) error {
	log.Printf("BACKING UP %s\n", fileDescriptor.RelativePath)

	client := http.Client{Timeout: time.Hour}
	fileContents, err := afero.ReadFile(c.fs, filepath.Join(
		c.folderPath,
		string(fileDescriptor.RelativePath)))
	if nil != err {
		return errors.Wrapf(err, "couldn't read file at %s", fileDescriptor.RelativePath)
	}

	protoBufFile := &protofiles.FileProto{
		Descriptor_: protobufs.FileDescriptorToProto(fileDescriptor),
		Contents:    fileContents,
	}

	marshalledFile, err := proto.Marshal(protoBufFile)
	if nil != err {
		return errors.Wrapf(err, "couldn't marshall file at %s to protobuf", fileDescriptor.RelativePath)
	}

	uploadURL := fmt.Sprintf("%s/api/buckets/%s/upload/%d/file",
		c.storeURL, c.bucketName, revisionStr)
	resp, err := client.Post(uploadURL, "application/octet-stream", bytes.NewBuffer(marshalledFile))
	if nil != err {
		return errors.Wrapf(err, "couldn't send file at %s to remote Store server", fileDescriptor.RelativePath)
	}
	defer resp.Body.Close()

	if 200 != resp.StatusCode {
		respBodyBytes, err := ioutil.ReadAll(resp.Body)
		if nil != err {
			respBodyBytes = []byte(fmt.Sprintf("couldn't read response body. Error: '%s'", err))
		}
		return fmt.Errorf("expected 200 (OK) repsonse code for file upload for '%s' to '%s', but received '%s'. Response Text: '%s'",
			string(fileDescriptor.RelativePath),
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
