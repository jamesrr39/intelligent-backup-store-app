package storewebserver

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
)

// StoreWebServer represents a handler handling requests for a store.
type StoreWebServer struct {
	store  *intelligentstore.IntelligentStore
	router *mux.Router
}

// NewStoreWebServer creates a StoreWebServer and sets up the routing for the services it provides.
func NewStoreWebServer(store *intelligentstore.IntelligentStore) *StoreWebServer {
	router := mux.NewRouter()

	bucketsHandler := NewBucketService(store)
	router.PathPrefix("/api/buckets/").Handler(http.StripPrefix("/api/buckets", bucketsHandler))
	router.PathPrefix("/").Handler(http.StripPrefix("/", http.FileServer(http.Dir("storewebserver/static")))) // TODO dev mode & production packaging

	//mainRouter.PathPrefix("/").Handler(http.StripPrefix("/", http.FileServer(http.Dir("static"))))

	return &StoreWebServer{store, router}
}

func (s *StoreWebServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
	r.Body.Close()
}
