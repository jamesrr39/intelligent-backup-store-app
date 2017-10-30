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

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protofiles "github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/protobufs/proto_files"
)

func testNowProvider() time.Time {
	return time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC)
}

func newTestBucketService(t *testing.T) *BucketService {
	mockStore := intelligentstore.NewMockStore(t, testNowProvider, afero.NewMemMapFs())
	return NewBucketService(mockStore.IntelligentStore)
}

func Test_handleGetAllBuckets(t *testing.T) {
	bucketService := newTestBucketService(t)

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

	var descriptors []*intelligentstore.FileDescriptor
	for _, testFile := range testFiles {
		descriptors = append(descriptors, testFile.toFileDescriptor(t))
	}

	store := intelligentstore.NewMockStore(t, testNowProvider, afero.NewMemMapFs())
	bucket, err := store.CreateBucket("docs")
	require.Nil(t, err)

	tx1, err := bucket.Begin(descriptors)
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

func Test_CreateRevision(t *testing.T) {
	aFileText := "my file a.txt"
	hashAtxt, err := intelligentstore.NewHash(bytes.NewBuffer([]byte(aFileText)))
	require.Nil(t, err)

	// Create a bucket and fill it with some data
	store := intelligentstore.NewMockStore(t, testNowProvider, afero.NewMemMapFs())
	bucket, err := store.CreateBucket("docs")
	require.Nil(t, err)

	tx1Descriptor, err := intelligentstore.NewFileDescriptorFromReader(
		"already-in_a.txt",
		bytes.NewBuffer([]byte(aFileText)))
	require.Nil(t, err)

	descriptors := []*intelligentstore.FileDescriptor{tx1Descriptor}

	tx1, err := bucket.Begin(descriptors)
	require.Nil(t, err)

	err = tx1.BackupFile(bytes.NewBuffer([]byte(aFileText)))
	require.Nil(t, err)

	err = tx1.Commit()
	require.Nil(t, err)
	// end filling it with data

	bucketService := NewBucketService(store.IntelligentStore)

	openTxRequest := &protofiles.OpenTxRequest{
		FileDescriptors: []*protofiles.FileDescriptorProto{
			&protofiles.FileDescriptorProto{
				Filename: "a.txt",
				Hash:     string(hashAtxt),
			},
			&protofiles.FileDescriptorProto{
				Filename: "mydir/b.txt",
				Hash:     "ijklmn",
			},
			&protofiles.FileDescriptorProto{
				Filename: "mydir/c.txt",
				Hash:     "ijklmn",
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

	require.Len(t, openTxResponse.GetHashes(), 1) // shouldn't ask for a.txt, as that is already in the store
	assert.Equal(t, "ijklmn", openTxResponse.GetHashes()[0])
}

func Test_handleUploadFile(t *testing.T) {
	aFileText := "my file a.txt"
	hashAtxt, err := intelligentstore.NewHash(bytes.NewBuffer([]byte(aFileText)))
	require.Nil(t, err)

	store := intelligentstore.NewMockStore(t, testNowProvider, afero.NewMemMapFs())
	_, err = store.CreateBucket("docs")
	require.Nil(t, err)

	// create a Revision
	openTxRequest := &protofiles.OpenTxRequest{
		FileDescriptors: []*protofiles.FileDescriptorProto{
			&protofiles.FileDescriptorProto{
				Filename: "a.txt",
				Hash:     string(hashAtxt),
			},
			&protofiles.FileDescriptorProto{
				Filename: "b.txt",
				Hash:     string(hashAtxt),
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

	uploadedFileProto := &protofiles.FileProto{
		Contents: []byte(aFileText),
		Hash:     string(hashAtxt),
	}
	uploadedFileProtoBytes, err := proto.Marshal(uploadedFileProto)
	require.Nil(t, err)

	r1 := &http.Request{
		Body:   ioutil.NopCloser(bytes.NewBuffer(uploadedFileProtoBytes)),
		URL:    &url.URL{Path: fmt.Sprintf("/docs/upload/%d/file", openTxResponse.GetRevisionID())},
		Method: "POST",
	}
	w1 := httptest.NewRecorder()

	// upload wanted file
	bucketService.ServeHTTP(w1, r1)
	assert.Equal(t, 200, w1.Code)

	// upload unwanted file
	unwantedUploadedFileProto := &protofiles.FileProto{
		Contents: []byte("unwanted file"),
		Hash:     "123",
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

type testfile struct {
	path     intelligentstore.RelativePath
	contents string
}

func (testFile *testfile) toFileDescriptor(t *testing.T) *intelligentstore.FileDescriptor {
	descriptor, err := intelligentstore.NewFileDescriptorFromReader(testFile.path, bytes.NewBuffer([]byte(testFile.contents)))
	require.Nil(t, err)
	return descriptor
}
