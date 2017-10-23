package storewebserver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/excludesmatcher"
	"github.com/jamesrr39/intelligent-backup-store-app/uploaders/localupload"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	fs := generateTestFSWithData(t, testFiles)
	store := intelligentstore.NewMockStore(t, testNowProvider, fs)
	bucketService := NewBucketService(store.IntelligentStore)
	_, err := bucketService.store.CreateBucket("docs")
	require.Nil(t, err)

	excludeMatcher, err := excludesmatcher.NewExcludesMatcherFromReader(
		bytes.NewBuffer([]byte("\nexclude*\n")))
	require.Nil(t, err)

	// create a new local uploader client
	uploader := &localupload.LocalUploader{
		BackupStore:        store.IntelligentStore,
		BackupBucketName:   "docs",
		BackupFromLocation: "/docs",
		ExcludeMatcher:     excludeMatcher,
		Fs:                 fs,
	}

	err = uploader.UploadToStore()
	require.Nil(t, err)

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

type testfile struct {
	path     intelligentstore.RelativePath
	contents string
}

func generateTestFSWithData(t *testing.T, testFiles []*testfile) afero.Fs {
	fs := afero.NewMemMapFs()
	err := fs.MkdirAll("/docs/folder1", 0700)
	require.Nil(t, err)

	for _, testFile := range testFiles {
		err = afero.WriteFile(fs, string("/docs/"+testFile.path), []byte(testFile.contents), 0600)
		require.Nil(t, err)
	}

	err = afero.WriteFile(fs, "/docs/excludefile.txt", []byte("file 1/c"), 0600)
	require.Nil(t, err)
	err = afero.WriteFile(fs, "/docs/excludeme/a.txt", []byte("file 1/c"), 0600)
	require.Nil(t, err)

	return fs
}
