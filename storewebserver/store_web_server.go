package storewebserver

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
)

// StoreWebServer represents a handler handling requests for a store.
type StoreWebServer struct {
	store *intelligentstore.IntelligentStore
	http.Handler
}

// NewStoreWebServer creates a StoreWebServer and sets up the routing for the services it provides.
func NewStoreWebServer(store *intelligentstore.IntelligentStore) *StoreWebServer {
	router := mux.NewRouter()

	bucketsHandler := NewBucketService(store)
	router.PathPrefix("/api/buckets/").Handler(http.StripPrefix("/api/buckets", bucketsHandler))
	router.PathPrefix("/").Handler(http.StripPrefix("/", http.FileServer(http.Dir("storewebserver/static")))) // TODO dev mode & production packaging

	return &StoreWebServer{store, router}
}
