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
	"github.com/jamesrr39/intelligent-backup-store-app/serialisation"
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
func (c *WebUploadClient) BackupFolder(folderPath string) error {
	// open transaction
	revisionStr, err := c.openTx()
	if nil != err {
		return err
	}
	log.Println("opened Tx. Rev: " + revisionStr)
	// backup files
	err = filepath.Walk(folderPath, func(path string, fileInfo os.FileInfo, err error) error {
		if nil != err {
			return err
		}

		if fileInfo.IsDir() {
			return nil
		}

		relativeFilePath := strings.TrimPrefix(path, folderPath)

		client := http.Client{Timeout: time.Hour}
		fileContents, err := ioutil.ReadFile(path)
		if nil != err {
			return err
		}

		protoBufFile := &serialisation.UploadedFile{
			Filename: relativeFilePath,
			File:     fileContents,
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
			return fmt.Errorf("expected 200 (OK) repsonse code for file upload for '%s' to '%s', but received '%s'. Response Text: '%s'", path, uploadURL, resp.Status, respBodyBytes)
		}

		log.Println("backed up " + path)
		return nil
	})
	if nil != err {
		return err
	}

	log.Println("commiting Tx")
	err = c.commitTx(revisionStr)
	if nil != err {
		return err
	}

	return nil
}

func (c *WebUploadClient) openTx() (string, error) {
	openTxClient := http.Client{Timeout: time.Second * 20}
	resp, err := openTxClient.Get(c.storeURL + "/api/buckets/" + c.bucketName + "/upload")
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
