package storewebserver

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nowProvider() time.Time {
	return time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC)
}

func newBucketService(t *testing.T) *BucketService {
	store := intelligentstore.NewMockStore(t, nowProvider)
	return NewBucketService(store)
}

func Test_handleGetAllBuckets(t *testing.T) {
	bucketService := newBucketService(t)

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
	assert.Equal(t, []byte("[{\"name\":\"docs\",\"lastRevisionTs\":null}]\n"), w2.Body.Bytes())

}
