package storewebserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protofiles "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/protobufs/proto_files"
)

func testNowProvider() time.Time {
	return time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC)
}

func Test_handleGetAllBuckets(t *testing.T) {
	mockStore := intelligentstore.NewMockStore(t, testNowProvider)
	bucketService := NewBucketService(mockStore.IntelligentStore)

	requestURL := &url.URL{Path: "/"}
	r1 := &http.Request{Method: "GET", URL: requestURL}
	w1 := httptest.NewRecorder()

	bucketService.ServeHTTP(w1, r1)

	assert.Equal(t, []byte("[]"), w1.Body.Bytes())
	assert.Equal(t, 200, w1.Code)

	_, err := bucketService.store.CreateBucket("docs")
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
	testFiles := []*testfile{
		&testfile{"a.txt", "file a"},
		&testfile{"b.txt", "file b"},
		&testfile{"folder-1/a.txt", "file 1/a"},
		&testfile{"folder-1/c.txt", "file 1/c"},
	}

	var fileInfos []*intelligentstore.FileInfo
	for _, testFile := range testFiles {
		fileInfos = append(fileInfos, testFile.toFileDescriptor(t).FileInfo)
	}

	store := intelligentstore.NewMockStore(t, testNowProvider)
	bucket, err := store.CreateBucket("docs")
	require.Nil(t, err)

	tx1, err := bucket.Begin(fileInfos)
	require.Nil(t, err)

	var relativePathsWithHashes []*intelligentstore.RelativePathWithHash
	for _, testFile := range testFiles {
		hash, err := intelligentstore.NewHash(bytes.NewBuffer([]byte(testFile.contents)))
		require.Nil(t, err)
		relativePathsWithHashes = append(
			relativePathsWithHashes,
			&intelligentstore.RelativePathWithHash{
				RelativePath: testFile.path,
				Hash:         hash,
			},
		)
	}

	_, err = tx1.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	for _, testFile := range testFiles {
		backupErr := tx1.BackupFile(bytes.NewBuffer([]byte(testFile.contents)))
		require.Nil(t, backupErr)
	}
	err = tx1.Commit()
	require.Nil(t, err)

	bucketService := NewBucketService(store.IntelligentStore)

	requestURL := &url.URL{Path: "/docs/latest"}
	r1 := &http.Request{Method: "GET", URL: requestURL}
	w1 := httptest.NewRecorder()

	bucketService.ServeHTTP(w1, r1)

	var revInfoWithFiles *revisionInfoWithFiles

	err = json.NewDecoder(w1.Body).Decode(&revInfoWithFiles)
	require.Nil(t, err)

	assert.Equal(t, 200, w1.Code)
	assert.Len(t, revInfoWithFiles.Files, 2)
	assert.Len(t, revInfoWithFiles.Dirs, 1)

	assert.Equal(t, "folder-1", revInfoWithFiles.Dirs[0].Name)
	assert.Equal(t, int64(2), revInfoWithFiles.Dirs[0].NestedFileCount)

	index := 0
	for _, fileDescriptor := range revInfoWithFiles.Files {
		assert.Equal(t, testFiles[index].path, fileDescriptor.RelativePath)
		index++
	}

	// request 2; testing with a rootDir
	r2 := &http.Request{Method: "GET", URL: &url.URL{Path: "/docs/latest", RawQuery: "rootDir=folder-1"}}
	w2 := httptest.NewRecorder()

	bucketService.ServeHTTP(w2, r2)

	var revInfoWithFiles2 *revisionInfoWithFiles

	err = json.NewDecoder(w2.Body).Decode(&revInfoWithFiles2)
	require.Nil(t, err)

	assert.Equal(t, 200, w2.Code)
	assert.Len(t, revInfoWithFiles2.Files, 2)
	assert.Len(t, revInfoWithFiles2.Dirs, 0)

	assert.Equal(t, testFiles[2].path, revInfoWithFiles2.Files[0].RelativePath)
	assert.Equal(t, testFiles[3].path, revInfoWithFiles2.Files[1].RelativePath)
}

func Test_handleCreateRevision(t *testing.T) {
	store := intelligentstore.NewMockStore(t, testNowProvider)
	_, err := store.CreateBucket("docs")
	require.Nil(t, err)

	aFileText := "test file a"
	aFileDescriptor, err := intelligentstore.NewFileDescriptorFromReader(
		"a.txt",
		time.Unix(0, 0),
		bytes.NewBuffer([]byte(aFileText)))
	require.Nil(t, err)

	bucketService := NewBucketService(store.IntelligentStore)

	openTxRequest := &protofiles.OpenTxRequest{
		FileInfos: []*protofiles.FileInfoProto{
			&protofiles.FileInfoProto{
				RelativePath: string(aFileDescriptor.RelativePath),
				ModTime:      aFileDescriptor.ModTime.Unix(),
				Size:         aFileDescriptor.Size,
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
	store := intelligentstore.NewMockStore(t, testNowProvider)
	_, err := store.CreateBucket("docs")
	require.Nil(t, err)

	aFileText := "my file a.txt"
	descriptor, err := intelligentstore.NewFileDescriptorFromReader(
		intelligentstore.NewRelativePath("a.txt"),
		time.Unix(0, 0),
		bytes.NewBuffer([]byte(aFileText)),
	)

	// create a Revision
	openTxRequest := &protofiles.OpenTxRequest{
		FileInfos: []*protofiles.FileInfoProto{
			&protofiles.FileInfoProto{
				RelativePath: string(descriptor.RelativePath),
				ModTime:      descriptor.ModTime.Unix(),
				Size:         descriptor.Size,
			},
		},
	}

	openTxRequestBytes, err := proto.Marshal(openTxRequest)
	require.Nil(t, err)

	bucketService := NewBucketService(store.IntelligentStore)

	openTxW := httptest.NewRecorder()
	openTxR := &http.Request{
		Method: "POST",
		URL:    &url.URL{Path: "/docs/upload"},
		Body:   ioutil.NopCloser(bytes.NewBuffer(openTxRequestBytes)),
	}

	bucketService.ServeHTTP(openTxW, openTxR)
	require.Equal(t, 200, openTxW.Code)

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

	r1 := &http.Request{
		URL:    &url.URL{Path: fmt.Sprintf("/docs/upload/%d/file", openTxResponse.GetRevisionID())},
		Method: "POST",
		Body:   ioutil.NopCloser(bytes.NewBuffer(uploadedFileProtoBytes)),
	}
	w1 := httptest.NewRecorder()

	// upload wanted file
	bucketService.ServeHTTP(w1, r1)
	log.Printf("%s\n", w1.Body.Bytes())
	require.Equal(t, 200, w1.Code)

	// upload unwanted file
	unwantedUploadedFileProto := &protofiles.FileContentsProto{
		Contents: []byte("unwanted file"),
	}
	unwantedUploadedFileProtoBytes, err := proto.Marshal(unwantedUploadedFileProto)
	require.Nil(t, err)

	rUnwanted := &http.Request{
		Body:   ioutil.NopCloser(bytes.NewBuffer(unwantedUploadedFileProtoBytes)),
		URL:    &url.URL{Path: fmt.Sprintf("/docs/upload/%d/file", openTxResponse.GetRevisionID())},
		Method: "POST",
	}
	wUnwanted := httptest.NewRecorder()

	bucketService.ServeHTTP(wUnwanted, rUnwanted)
	assert.Equal(t, 400, wUnwanted.Code)
	assert.Equal(
		t,
		intelligentstore.ErrFileNotRequiredForTransaction.Error(),
		strings.TrimSuffix(string(wUnwanted.Body.Bytes()), "\n"),
	)

	// upload file already uploaded
	rAlreadyUploaded := &http.Request{
		Body:   ioutil.NopCloser(bytes.NewBuffer(uploadedFileProtoBytes)),
		URL:    &url.URL{Path: fmt.Sprintf("/docs/upload/%d/file", openTxResponse.GetRevisionID())},
		Method: "POST",
	}
	wAlreadyUploaded := httptest.NewRecorder()

	// upload wanted file
	bucketService.ServeHTTP(wAlreadyUploaded, rAlreadyUploaded)
	assert.Equal(t, 400, wAlreadyUploaded.Code)
	assert.Equal(
		t,
		intelligentstore.ErrFileNotRequiredForTransaction.Error(),
		strings.TrimSuffix(string(wAlreadyUploaded.Body.Bytes()), "\n"),
	)
}

func Test_handleCommitTransaction(t *testing.T) {
	store := intelligentstore.NewMockStore(t, testNowProvider)
	bucket, err := store.CreateBucket("docs")
	require.Nil(t, err)

	bucketService := NewBucketService(store.IntelligentStore)

	bucketRevisions, err := bucket.GetRevisions()
	require.Nil(t, err)
	require.Len(t, bucketRevisions, 0)

	fileContents := "file contents of a öøæäå"
	descriptor, err := intelligentstore.NewFileDescriptorFromReader(
		intelligentstore.NewRelativePath("my/file a.txt"),
		time.Unix(0, 0),
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

	bucketRevisions, err = bucket.GetRevisions()
	require.Nil(t, err)
	require.Len(t, bucketRevisions, 1)
	assert.Equal(t,
		openTxResponse.GetRevisionID(),
		int64(bucketRevisions[0].VersionTimestamp))

}

func Test_handleGetFileContents(t *testing.T) {
	mockStore := intelligentstore.NewMockStore(t, testNowProvider)
	bucket, err := mockStore.CreateBucket("docs")
	require.Nil(t, err)

	bucketService := NewBucketService(mockStore.IntelligentStore)

	fileContents := "my file contents"
	fileName := "folder1/file a.txt"
	descriptor, err := intelligentstore.NewFileDescriptorFromReader(
		intelligentstore.NewRelativePath(fileName),
		time.Unix(0, 0),
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

	tx, err := bucket.Begin(fileInfos)
	require.Nil(t, err)

	relativePathsWithHashes := []*intelligentstore.RelativePathWithHash{
		&intelligentstore.RelativePathWithHash{
			RelativePath: descriptor.RelativePath,
			Hash:         descriptor.Hash,
		},
	}

	_, err = tx.ProcessUploadHashesAndGetRequiredHashes(relativePathsWithHashes)
	require.Nil(t, err)

	err = tx.BackupFile(bytes.NewBuffer([]byte(fileContents)))
	require.Nil(t, err)

	err = tx.Commit()
	require.Nil(t, err)

	file, err := mockStore.GetObjectByHash(descriptor.Hash)
	require.Nil(t, err)
	defer file.Close()

	rRevisionExistsButFileDoesNotExist := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Path:     fmt.Sprintf("/docs/%d/file", tx.VersionTimestamp),
			RawQuery: fmt.Sprintf("relativePath=notexist_%s", fileName),
		},
	}
	wRevisionExistsButFileDoesNotExist := httptest.NewRecorder()

	bucketService.ServeHTTP(wRevisionExistsButFileDoesNotExist, rRevisionExistsButFileDoesNotExist)
	assert.Equal(t, 404, wRevisionExistsButFileDoesNotExist.Code)

	rExists := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Path:     fmt.Sprintf("/docs/%d/file", tx.VersionTimestamp),
			RawQuery: fmt.Sprintf("relativePath=%s", url.QueryEscape(fileName)),
		},
	}
	wExists := httptest.NewRecorder()

	bucketService.ServeHTTP(wExists, rExists)
	require.Equal(t, 200, wExists.Code)
	require.Equal(t, fileContents, string(wExists.Body.Bytes()))
}

type testfile struct {
	path     intelligentstore.RelativePath
	contents string
}

func (testFile *testfile) toFileDescriptor(t *testing.T) *intelligentstore.FileDescriptor {
	descriptor, err := intelligentstore.NewFileDescriptorFromReader(
		testFile.path,
		time.Unix(0, 0),
		bytes.NewBuffer([]byte(testFile.contents)),
	)
	require.Nil(t, err)
	return descriptor
}
