package storewebserver

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
)

// StoreWebServer represents a handler handling requests for a store.
type StoreWebServer struct {
	store *intelligentstore.IntelligentStoreDAL
	http.Handler
}

// NewStoreWebServer creates a StoreWebServer and sets up the routing for the services it provides.
func NewStoreWebServer(store *intelligentstore.IntelligentStoreDAL) *StoreWebServer {
	router := mux.NewRouter()
	storeHandler := &StoreWebServer{store, router}

	router.HandleFunc("/api/search", storeHandler.handleSearch)

	bucketsHandler := NewBucketService(store)
	router.PathPrefix("/api/buckets/").Handler(http.StripPrefix("/api/buckets", bucketsHandler))
	router.PathPrefix("/").Handler(http.StripPrefix("/", http.FileServer(http.Dir("storewebserver/static")))) // TODO dev mode & production packaging

	return storeHandler
}

func (s *StoreWebServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	searchTerm := r.URL.Query().Get("searchTerm")
	if "" == searchTerm {
		w.WriteHeader(400)
		w.Write([]byte("no search term specified (use URL query parameter `searchTerm`)"))
		return
	}

	// go through all the buckets and all revisions
	searchResults, err := s.store.Search(searchTerm)
	if nil != err {
		w.WriteHeader(500)
		w.Write([]byte(fmt.Sprintf("couldn't search store. Error: %s", err)))
		return
	}

	w.Header().Set("content-type", "application/json")
	if 0 == len(searchResults) {
		w.Write([]byte("[]"))
		return
	}

	err = json.NewEncoder(w).Encode(searchResults)
	if nil != err {
		fmt.Fprintf(w, "couldn't serialize search results to JSON. Error: %s", err)
		return
	}
}
