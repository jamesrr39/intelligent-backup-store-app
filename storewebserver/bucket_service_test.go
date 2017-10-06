package storewebserver

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
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
