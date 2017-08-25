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
	"github.com/jamesrr39/intelligent-backup-store-app/serialisation/protogenerated"
)

// WebUploadClient represents an http client for uploading files to an IntelligentStore
type WebUploadClient struct {
	storeURL   string
	bucketName string
}

// NewWebUploadClient creates a new WebUploadClient
func NewWebUploadClient(storeURL, bucketName string) *WebUploadClient {
	return &WebUploadClient{storeURL, bucketName}
}

// BackupFolder backs up a directory on the local machine to the bucket in the store in the WebUploadClient
func (c *WebUploadClient) BackupFolder(folderPath string, excludeFilesMatcher *excludesmatcher.ExcludesMatcher, dryRun bool) error {
	var fileList []*intelligentstore.File

	var totalFilesToUpload int64
	var totalBytesToUpload int64

	// build file list
	err := filepath.Walk(folderPath, func(path string, fileInfo os.FileInfo, err error) error {
		if nil != err {
			return err
		}

		if fileInfo.IsDir() {
			return nil
		}

		relativeFilePath := strings.TrimPrefix(strings.TrimPrefix(path, folderPath), string(filepath.Separator))

		if excludeFilesMatcher.Matches(relativeFilePath) {
			// skip excluded file
			log.Printf("skipping %s\n", relativeFilePath)
			return nil
		}

		totalFilesToUpload++
		totalBytesToUpload += fileInfo.Size()

		fileDescriptor, err := intelligentstore.NewFileFromFilePath(path)
		if nil != err {
			return err
		}

		fileList = append(fileList, fileDescriptor)

		return nil
	})
	if nil != err {
		return err
	}

	if dryRun {
		log.Printf("files to back up: %d, %d bytes.\nExiting (dry run)",
			totalFilesToUpload,
			totalBytesToUpload)
		return nil
	}

	// open transaction
	revisionStr, err := c.openTx(fileList)
	if nil != err {
		return err
	}
	log.Println("opened Tx. Rev: " + revisionStr)

	var filesSuccessfullyBackedUpCount int64
	var filesFailedToBackup []*intelligentstore.File

	for _, fileDescriptor := range fileList {
		err = c.backupFile(folderPath, revisionStr, fileDescriptor)
		if nil != err {
			filesFailedToBackup = append(filesFailedToBackup, fileDescriptor)
			log.Printf("failed to backup %s. Error: %s\n", fileDescriptor, err)
		} else {
			filesSuccessfullyBackedUpCount++
		}

		filesProcessedSoFar := filesSuccessfullyBackedUpCount + int64(len(filesFailedToBackup))
		if (filesProcessedSoFar % 10) == 0 {
			log.Printf("%d of %d files processed\n", filesProcessedSoFar, totalFilesToUpload)
		}
	}

	log.Println("commiting Tx")
	err = c.commitTx(revisionStr)
	if nil != err {
		return err
	}

	return nil
}

func (c *WebUploadClient) backupFile(basePath, revisionStr string, fileDescriptor *intelligentstore.File) error {

	client := http.Client{Timeout: time.Hour}
	fileContents, err := ioutil.ReadFile(fileDescriptor.FilePath)
	if nil != err {
		return err
	}

	protoBufFile := &protogenerated.FileProto{
		Descriptor_: descriptorToProto(fileDescriptor),
		Contents:    fileContents,
	}

	marshalledFile, err := proto.Marshal(protoBufFile)
	if nil != err {
		return err
	}

	uploadURL := fmt.Sprintf("%s/api/buckets/%s/upload/%s/file",
		c.storeURL, c.bucketName, revisionStr)
	resp, err := client.Post(uploadURL, "application/octet-stream", bytes.NewBuffer(marshalledFile))
	if nil != err {
		return err
	}
	defer resp.Body.Close()

	if 200 != resp.StatusCode {
		respBodyBytes, err := ioutil.ReadAll(resp.Body)
		if nil != err {
			respBodyBytes = []byte(fmt.Sprintf("couldn't read response body. Error: '%s'", err))
		}
		return fmt.Errorf("expected 200 (OK) repsonse code for file upload for '%s' to '%s', but received '%s'. Response Text: '%s'",
			fileDescriptor.FilePath,
			uploadURL,
			resp.Status,
			respBodyBytes)
	}
	return nil
}

// openTx opens a transaction with the server and sends a list of files it wants to back up
func (c *WebUploadClient) openTx(fileDescriptors []*intelligentstore.File) (string, error) {
	var protoFileDescriptors *protogenerated.FileDescriptorProtoList
	for _, descriptor := range fileDescriptors {
		descriptorProto := &protogenerated.FileDescriptorProto{
			Filename: descriptor.FilePath,
			Hash:     string(descriptor.Hash),
		}

		protoFileDescriptors.FileDescriptorList = append(protoFileDescriptors.FileDescriptorList, descriptorProto)
	}

	fileDescriptorListBytes, err := proto.Marshal(protoFileDescriptors)
	if nil != err {
		return "", err
	}

	openTxClient := http.Client{Timeout: time.Second * 20}

	openTxURL := c.storeURL + "/api/buckets/" + c.bucketName + "/upload"
	resp, err := openTxClient.Post(openTxURL, "application/octet-stream", bytes.NewBuffer(fileDescriptorListBytes))
	if nil != err {
		return "", err
	}
	defer resp.Body.Close()

	if 200 != resp.StatusCode {
		return "", fmt.Errorf("expected 200 (OK) repsonse code for open transaction, but received '%s'", resp.Status)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return "", err
	}

	var openTxResponse protogenerated.OpenTxResponse
	err = proto.Unmarshal(respBytes, &openTxResponse)
	if nil != err {
		return "", err
	}

	return string(respBytes), nil
}

func (c *WebUploadClient) commitTx(revisionStr string) error {
	commitTxClient := http.Client{Timeout: time.Second * 20}
	resp, err := commitTxClient.Get(c.storeURL + "/api/buckets/" + c.bucketName + "/upload/" + revisionStr + "/commit")
	if nil != err {
		return err
	}
	defer resp.Body.Close()

	if 200 != resp.StatusCode {
		return fmt.Errorf("expected 200 (OK) repsonse code for commit, but received '%s'", resp.Status)
	}

	return nil
}

func descriptorToProto(descriptor *intelligentstore.File) *protogenerated.FileDescriptorProto {
	return &protogenerated.FileDescriptorProto{
		Filename: descriptor.FilePath,
		Hash:     string(descriptor.Hash),
	}
}
