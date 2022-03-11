package storewebserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jamesrr39/goutil/gofs/mockfs"
	"github.com/jamesrr39/goutil/logpkg"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_handleSearch(t *testing.T) {
	logger := logpkg.NewLogger(os.Stderr, logpkg.LogLevelInfo)

	store := dal.NewMockStore(t, dal.MockNowProvider, mockfs.NewMockFs())
	bucket := storetest.CreateBucket(t, store.Store, "docs")

	revision := storetest.CreateRevision(t, store.Store, bucket, []*intelligentstore.RegularFileDescriptorWithContents{
		intelligentstore.NewRegularFileDescriptorWithContents(t, intelligentstore.NewRelativePath("a/contract.txt"), time.Unix(0, 0), dal.FileMode600, []byte("")),
		intelligentstore.NewRegularFileDescriptorWithContents(t, intelligentstore.NewRelativePath("a/something else.txt"), time.Unix(0, 0), dal.FileMode600, []byte("")),
	})

	storeHandler := NewStoreWebServer(logger, store.Store)

	// good request

	w1 := httptest.NewRecorder()
	r1 := &http.Request{
		URL: &url.URL{
			Path:     "/api/search",
			RawQuery: "searchTerm=contract",
		},
	}

	storeHandler.ServeHTTP(w1, r1)

	var results []*intelligentstore.SearchResult
	err := json.Unmarshal(w1.Body.Bytes(), &results)
	require.Nil(t, err)

	require.Equal(t, 200, w1.Code)
	require.Len(t, results, 1)
	assert.Equal(t, "a/contract.txt", string(results[0].RelativePath))
	assert.Equal(t, bucket.BucketName, results[0].Bucket.BucketName)
	assert.Equal(t, revision.VersionTimestamp, results[0].Revision.VersionTimestamp)

	// bad request (no search term specified)

	w2 := httptest.NewRecorder()
	r2 := &http.Request{
		URL: &url.URL{
			Path:     "/api/search",
			RawQuery: "",
		},
	}

	storeHandler.ServeHTTP(w2, r2)

	require.Equal(t, 400, w2.Code)
	assert.Equal(t, "no search term specified (use URL query parameter `searchTerm`)\n", string(w2.Body.Bytes()))

	// good request, no results

	w3 := httptest.NewRecorder()
	r3 := &http.Request{
		URL: &url.URL{
			Path:     "/api/search",
			RawQuery: "searchTerm=me.txt",
		},
	}

	storeHandler.ServeHTTP(w3, r3)

	require.Equal(t, 200, w3.Code)
	assert.Equal(t, "[]\n", string(w3.Body.Bytes()))
}
