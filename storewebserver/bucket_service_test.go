package storewebserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/jamesrr39/goutil/gofs/mockfs"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	protofiles "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/protobufs/proto_files"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testNowProvider() time.Time {
	return time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC)
}

func Test_handleGetAllBuckets(t *testing.T) {
	mockStore := dal.NewMockStore(t, testNowProvider, mockfs.NewMockFs())
	bucketService := NewBucketService(mockStore.Store)

	requestURL := &url.URL{Path: "/"}
	r1 := &http.Request{Method: "GET", URL: requestURL}
	w1 := httptest.NewRecorder()

	bucketService.ServeHTTP(w1, r1)

	assert.Equal(t, []byte("[]"), w1.Body.Bytes())
	assert.Equal(t, 200, w1.Code)

	_, err := bucketService.store.BucketDAL.CreateBucket("docs")
	require.Nil(t, err)

	r2 := &http.Request{Method: "GET", URL: requestURL}
	w2 := httptest.NewRecorder()

	bucketService.ServeHTTP(w2, r2)

	assert.Equal(t, 200, w2.Code)
	assert.Equal(t,
		`[{"name":"docs","lastRevisionTs":null}]`,
		strings.TrimSuffix(w2.Body.String(), "\n"))

	r3 := &http.Request{Method: "GET", URL: &url.URL{Path: "/docs"}}
	w3 := httptest.NewRecorder()

	bucketService.ServeHTTP(w3, r3)

	assert.Equal(t, 200, w3.Code)
	assert.Equal(t, `{"revisions":[]}`, strings.TrimSuffix(w3.Body.String(), "\n"))
}

func Test_handleGetRevision(t *testing.T) {
	// create the fs, and put some test data in it
	testFiles := []*intelligentstore.RegularFileDescriptorWithContents{
		intelligentstore.NewRegularFileDescriptorWithContents(t, "a.txt", time.Unix(0, 0), dal.FileMode600, []byte("file a")),
		intelligentstore.NewRegularFileDescriptorWithContents(t, "b.txt", time.Unix(0, 0), dal.FileMode600, []byte("file b")),
		intelligentstore.NewRegularFileDescriptorWithContents(t, "folder-1/a.txt", time.Unix(0, 0), dal.FileMode600, []byte("file 1/a")),
		intelligentstore.NewRegularFileDescriptorWithContents(t, "folder-1/c.txt", time.Unix(0, 0), dal.FileMode600, []byte("file 1/c")),
	}

	store := dal.NewMockStore(t, testNowProvider, mockfs.NewMockFs())
	bucket := store.CreateBucket(t, "docs")

	store.CreateRevision(t, bucket, testFiles)

	bucketService := NewBucketService(store.Store)

	requestURL := &url.URL{Path: "/docs/latest"}
	r1 := &http.Request{Method: "GET", URL: requestURL}
	w1 := httptest.NewRecorder()

	bucketService.ServeHTTP(w1, r1)

	var revInfoWithFiles *revisionInfoWithFiles

	err := json.NewDecoder(w1.Body).Decode(&revInfoWithFiles)
	require.Nil(t, err)
	require.Equal(t, 200, w1.Code)
	require.Len(t, revInfoWithFiles.Files, 2)
	require.Len(t, revInfoWithFiles.Dirs, 1)

	assert.Equal(t, "folder-1", revInfoWithFiles.Dirs[0].Name)
	assert.Equal(t, int64(2), revInfoWithFiles.Dirs[0].NestedFileCount)

	require.Len(t, revInfoWithFiles.Files, 2)

	assert.True(t, ((revInfoWithFiles.Files[0].GetFileInfo().RelativePath == testFiles[0].Descriptor.RelativePath && revInfoWithFiles.Files[1].GetFileInfo().RelativePath == testFiles[1].Descriptor.RelativePath) ||
		(revInfoWithFiles.Files[0].GetFileInfo().RelativePath == testFiles[1].Descriptor.RelativePath && revInfoWithFiles.Files[1].GetFileInfo().RelativePath == testFiles[0].Descriptor.RelativePath)))

	// request 2; testing with a rootDir
	r2 := &http.Request{Method: "GET", URL: &url.URL{Path: "/docs/latest", RawQuery: "rootDir=folder-1"}}
	w2 := httptest.NewRecorder()

	bucketService.ServeHTTP(w2, r2)

	var revInfoWithFiles2 *revisionInfoWithFiles

	err = json.NewDecoder(w2.Body).Decode(&revInfoWithFiles2)
	require.Nil(t, err)

	assert.Equal(t, 200, w2.Code)
	require.Len(t, revInfoWithFiles2.Files, 2)
	require.Len(t, revInfoWithFiles2.Dirs, 0)

	receivedRelativePath0 := revInfoWithFiles2.Files[0].GetFileInfo().RelativePath
	receivedRelativePath1 := revInfoWithFiles2.Files[1].GetFileInfo().RelativePath

	relativePathsAreRecieved := testFiles[2].Descriptor.RelativePath == receivedRelativePath0 && testFiles[3].Descriptor.RelativePath == receivedRelativePath1 ||
		testFiles[2].Descriptor.RelativePath == receivedRelativePath1 && testFiles[3].Descriptor.RelativePath == receivedRelativePath0
	assert.True(t, relativePathsAreRecieved)
}

func Test_handleCreateRevision(t *testing.T) {
	store := dal.NewMockStore(t, testNowProvider, mockfs.NewMockFs())
	store.CreateBucket(t, "docs")

	aFileText := "test file a"
	aFileDescriptor, err := intelligentstore.NewRegularFileDescriptorFromReader(
		"a.txt",
		time.Unix(0, 0),
		dal.FileMode600,
		bytes.NewBuffer([]byte(aFileText)))
	require.Nil(t, err)

	bucketService := NewBucketService(store.Store)

	openTxRequest := &protofiles.OpenTxRequest{
		FileInfos: []*protofiles.FileInfoProto{
			&protofiles.FileInfoProto{
				RelativePath: string(aFileDescriptor.RelativePath),
				ModTime:      aFileDescriptor.ModTime.Unix(),
				Size:         aFileDescriptor.Size,
				FileType:     protofiles.FileType(aFileDescriptor.Type),
			},
		},
	}

	openTxRequestBytes, err := proto.Marshal(openTxRequest)
	require.Nil(t, err)

	r1 := &http.Request{
		Method: "POST",
		URL:    &url.URL{Path: "/docs/upload"},
		Body:   ioutil.NopCloser(bytes.NewBuffer(openTxRequestBytes)),
	}
	w1 := httptest.NewRecorder()

	bucketService.ServeHTTP(w1, r1)

	assert.Equal(t, 200, w1.Code)

	var openTxResponse protofiles.OpenTxResponse
	err = proto.Unmarshal(w1.Body.Bytes(), &openTxResponse)
	require.Nil(t, err)

	assert.Equal(t, int64(946782245), openTxResponse.GetRevisionID())
	require.Len(t, openTxResponse.GetRequiredRelativePaths(), 1)
}

func Test_handleUploadFile(t *testing.T) {
	store := dal.NewMockStore(t, testNowProvider, mockfs.NewMockFs())
	store.CreateBucket(t, "docs")

	aFileText := "my file a.txt"
	descriptor, err := intelligentstore.NewRegularFileDescriptorFromReader(
		intelligentstore.NewRelativePath("a.txt"),
		time.Unix(0, 0),
		dal.FileMode600,
		bytes.NewBuffer([]byte(aFileText)),
	)

	// create a Revision
	openTxRequest := &protofiles.OpenTxRequest{
		FileInfos: []*protofiles.FileInfoProto{
			&protofiles.FileInfoProto{
				RelativePath: string(descriptor.RelativePath),
				ModTime:      descriptor.ModTime.Unix(),
				Size:         descriptor.Size,
				FileType:     protofiles.FileType(descriptor.Type),
			},
		},
	}

	openTxRequestBytes, err := proto.Marshal(openTxRequest)
	require.Nil(t, err)

	bucketService := NewBucketService(store.Store)

	openTxW := httptest.NewRecorder()
	openTxR := &http.Request{
		Method: "POST",
		URL:    &url.URL{Path: "/docs/upload"},
		Body:   ioutil.NopCloser(bytes.NewBuffer(openTxRequestBytes)),
	}

	bucketService.ServeHTTP(openTxW, openTxR)
	require.Equal(t, http.StatusOK, openTxW.Code)

	var openTxResponse protofiles.OpenTxResponse
	err = proto.Unmarshal(openTxW.Body.Bytes(), &openTxResponse)
	require.Nil(t, err)

	hashesRequestProto := &protofiles.GetRequiredHashesRequest{
		RelativePathsAndHashes: []*protofiles.RelativePathAndHashProto{
			&protofiles.RelativePathAndHashProto{
				RelativePath: string(descriptor.RelativePath),
				Hash:         string(descriptor.Hash),
			},
		},
	}
	hashesRequestBytes, err := proto.Marshal(hashesRequestProto)
	require.Nil(t, err)

	rHashes := &http.Request{
		URL:    &url.URL{Path: fmt.Sprintf("/docs/upload/%d/hashes", openTxResponse.GetRevisionID())},
		Method: "POST",
		Body:   ioutil.NopCloser(bytes.NewBuffer(hashesRequestBytes)),
	}
	wHashes := httptest.NewRecorder()

	bucketService.ServeHTTP(wHashes, rHashes)

	uploadedFileProto := &protofiles.FileContentsProto{
		Contents: []byte(aFileText),
	}
	uploadedFileProtoBytes, err := proto.Marshal(uploadedFileProto)
	require.Nil(t, err)

	t.Run("wanted file", func(t *testing.T) {
		r1 := &http.Request{
			URL:    &url.URL{Path: fmt.Sprintf("/docs/upload/%d/file", openTxResponse.GetRevisionID())},
			Method: "POST",
			Body:   ioutil.NopCloser(bytes.NewBuffer(uploadedFileProtoBytes)),
		}
		w1 := httptest.NewRecorder()

		// upload wanted file
		bucketService.ServeHTTP(w1, r1)
		require.Equal(t, 200, w1.Code)
	})

	t.Run("unwanted file", func(t *testing.T) {
		// upload unwanted file
		unwantedUploadedFileProto := &protofiles.FileContentsProto{
			Contents: []byte("unwanted file"),
		}
		unwantedUploadedFileProtoBytes, err := proto.Marshal(unwantedUploadedFileProto)
		require.Nil(t, err)

		rUnwanted := &http.Request{
			Body:   ioutil.NopCloser(bytes.NewBuffer(unwantedUploadedFileProtoBytes)),
			URL:    &url.URL{Path: fmt.Sprintf("/docs/upload/%d/file", openTxResponse.GetRevisionID())},
			Method: http.MethodPost,
		}
		wUnwanted := httptest.NewRecorder()

		bucketService.ServeHTTP(wUnwanted, rUnwanted)
		assert.Equal(t, 400, wUnwanted.Code)
		assert.Equal(
			t,
			dal.ErrFileNotRequiredForTransaction.Error(),
			strings.TrimSuffix(string(wUnwanted.Body.Bytes()), "\n"),
		)
	})

	t.Run("already uploaded file", func(t *testing.T) {
		rAlreadyUploaded := &http.Request{
			Body:   ioutil.NopCloser(bytes.NewBuffer(uploadedFileProtoBytes)),
			URL:    &url.URL{Path: fmt.Sprintf("/docs/upload/%d/file", openTxResponse.GetRevisionID())},
			Method: http.MethodPost,
		}
		wAlreadyUploaded := httptest.NewRecorder()

		bucketService.ServeHTTP(wAlreadyUploaded, rAlreadyUploaded)
		assert.Equal(t, 400, wAlreadyUploaded.Code)
		assert.Equal(
			t,
			dal.ErrFileNotRequiredForTransaction.Error(),
			strings.TrimSuffix(string(wAlreadyUploaded.Body.Bytes()), "\n"),
		)
	})
}

func Test_handleCommitTransaction(t *testing.T) {
	store := dal.NewMockStore(t, testNowProvider, mockfs.NewMockFs())
	bucket := store.CreateBucket(t, "docs")

	bucketService := NewBucketService(store.Store)

	bucketRevisions, err := store.Store.RevisionDAL.GetRevisions(bucket)
	require.Nil(t, err)
	require.Len(t, bucketRevisions, 0)

	fileContents := "file contents of a öøæäå"
	descriptor, err := intelligentstore.NewRegularFileDescriptorFromReader(
		intelligentstore.NewRelativePath("my/file a.txt"),
		time.Unix(0, 0),
		dal.FileMode600,
		bytes.NewBuffer([]byte(fileContents)),
	)
	require.Nil(t, err)

	// create a Revision
	openTxRequest := &protofiles.OpenTxRequest{
		FileInfos: []*protofiles.FileInfoProto{
			&protofiles.FileInfoProto{
				RelativePath: string(descriptor.RelativePath),
				ModTime:      descriptor.ModTime.Unix(),
				Size:         descriptor.Size,
				FileType:     protofiles.FileType(descriptor.Type),
			},
		},
	}

	openTxRequestBytes, err := proto.Marshal(openTxRequest)
	require.Nil(t, err)

	// open transaction
	openTxW := httptest.NewRecorder()
	openTxR := &http.Request{
		Method: "POST",
		URL:    &url.URL{Path: "/docs/upload"},
		Body:   ioutil.NopCloser(bytes.NewBuffer(openTxRequestBytes)),
	}

	bucketService.ServeHTTP(openTxW, openTxR)

	var openTxResponse protofiles.OpenTxResponse
	err = proto.Unmarshal(openTxW.Body.Bytes(), &openTxResponse)
	require.Nil(t, err)

	require.Len(t, openTxResponse.GetRequiredRelativePaths(), 1)
	require.Equal(t, string(descriptor.RelativePath), openTxResponse.GetRequiredRelativePaths()[0])

	getRequiredHashesBytes, err := proto.Marshal(&protofiles.GetRequiredHashesRequest{
		RelativePathsAndHashes: []*protofiles.RelativePathAndHashProto{
			&protofiles.RelativePathAndHashProto{
				RelativePath: string(descriptor.RelativePath),
				Hash:         string(descriptor.Hash),
			},
		},
	})
	require.Nil(t, err)

	rHashes := &http.Request{
		URL:    &url.URL{Path: fmt.Sprintf("/docs/upload/%d/hashes", openTxResponse.GetRevisionID())},
		Method: "POST",
		Body:   ioutil.NopCloser(bytes.NewBuffer(getRequiredHashesBytes)),
	}
	wHashes := httptest.NewRecorder()

	bucketService.ServeHTTP(wHashes, rHashes)

	// upload file
	uploadFileProto := &protofiles.FileContentsProto{
		Contents: []byte(fileContents),
	}

	uploadFileProtoBytes, err := proto.Marshal(uploadFileProto)
	require.Nil(t, err)

	uploadFileW := httptest.NewRecorder()
	uploadFileR := &http.Request{
		Method: "POST",
		URL: &url.URL{
			Path: fmt.Sprintf("/docs/upload/%d/file", openTxResponse.GetRevisionID()),
		},
		Body: ioutil.NopCloser(bytes.NewBuffer(uploadFileProtoBytes)),
	}

	bucketService.ServeHTTP(uploadFileW, uploadFileR)

	// commit transaction
	commitTxW := httptest.NewRecorder()
	commitTxR := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Path: fmt.Sprintf("/docs/upload/%d/commit", openTxResponse.GetRevisionID()),
		},
	}

	bucketService.ServeHTTP(commitTxW, commitTxR)
	require.Equal(t, 200, commitTxW.Code)

	bucketRevisions, err = store.Store.RevisionDAL.GetRevisions(bucket)
	require.Nil(t, err)
	require.Len(t, bucketRevisions, 1)
	assert.Equal(t,
		openTxResponse.GetRevisionID(),
		int64(bucketRevisions[0].VersionTimestamp))

}

func Test_handleGetFileContents(t *testing.T) {
	mockStore := dal.NewMockStore(t, testNowProvider, mockfs.NewMockFs())
	bucket := mockStore.CreateBucket(t, "docs")

	bucketService := NewBucketService(mockStore.Store)

	fileContents := "my file contents"
	fileName := "folder1/file a.txt"
	descriptor, err := intelligentstore.NewRegularFileDescriptorFromReader(
		intelligentstore.NewRelativePath(fileName),
		time.Unix(0, 0),
		dal.FileMode600,
		bytes.NewBuffer([]byte(fileContents)))
	require.Nil(t, err)

	rBucketNotExist := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Path:     "/bad-bucket/1234/file",
			RawQuery: fmt.Sprintf("relativePath=%s", fileName),
		},
	}
	wBucketNotExist := httptest.NewRecorder()

	bucketService.ServeHTTP(wBucketNotExist, rBucketNotExist)
	assert.Equal(t, 404, wBucketNotExist.Code)

	rRevisionNotExist := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Path:     "/docs/1234/file",
			RawQuery: fmt.Sprintf("relativePath=%s", fileName),
		},
	}
	wRevisionNotExist := httptest.NewRecorder()

	bucketService.ServeHTTP(wRevisionNotExist, rRevisionNotExist)
	assert.Equal(t, 404, wRevisionNotExist.Code)

	fileInfos := []*intelligentstore.FileInfo{descriptor.FileInfo}

	tx, err := mockStore.Store.TransactionDAL.CreateTransaction(bucket, fileInfos)
	require.Nil(t, err)

	relativePathsWithHashes := []*intelligentstore.RelativePathWithHash{
		&intelligentstore.RelativePathWithHash{
			RelativePath: descriptor.RelativePath,
			Hash:         descriptor.Hash,
		},
	}

	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.BackupFile(tx, bytes.NewReader([]byte(fileContents)))
	require.Nil(t, err)

	err = mockStore.Store.TransactionDAL.Commit(tx)
	require.Nil(t, err)

	file, err := mockStore.Store.GetGzippedObjectByHash(descriptor.Hash)
	require.Nil(t, err)
	defer file.Close()

	rRevisionExistsButFileDoesNotExist := &http.Request{
		Method: http.MethodGet,
		URL: &url.URL{
			Path:     fmt.Sprintf("/docs/%d/file", tx.Revision.VersionTimestamp),
			RawQuery: fmt.Sprintf("relativePath=notexist_%s", fileName),
		},
	}
	wRevisionExistsButFileDoesNotExist := httptest.NewRecorder()

	bucketService.ServeHTTP(wRevisionExistsButFileDoesNotExist, rRevisionExistsButFileDoesNotExist)
	assert.Equal(t, 404, wRevisionExistsButFileDoesNotExist.Code)

	rExists := &http.Request{
		Method: http.MethodGet,
		URL: &url.URL{
			Path:     fmt.Sprintf("/docs/%d/file", tx.Revision.VersionTimestamp),
			RawQuery: fmt.Sprintf("relativePath=%s", url.QueryEscape(fileName)),
		},
	}
	wExists := httptest.NewRecorder()

	bucketService.ServeHTTP(wExists, rExists)
	require.Equal(t, 200, wExists.Code)

	require.Equal(t, fileContents, wExists.Body.String())
}
