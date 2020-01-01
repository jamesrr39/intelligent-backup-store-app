package storewebserver

import (
	"fmt"
	"net/http"

	"github.com/go-chi/render"
	"github.com/gorilla/mux"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

// StoreWebServer represents a handler handling requests for a store.
type StoreWebServer struct {
	store *dal.IntelligentStoreDAL
	http.Handler
}

// NewStoreWebServer creates a StoreWebServer and sets up the routing for the services it provides.
func NewStoreWebServer(store *dal.IntelligentStoreDAL) *StoreWebServer {
	router := mux.NewRouter()
	storeHandler := &StoreWebServer{store, router}

	router.HandleFunc("/api/search", storeHandler.handleSearch)

	bucketsHandler := NewBucketService(store)
	router.PathPrefix("/api/buckets/").Handler(http.StripPrefix("/api/buckets", bucketsHandler))
	router.PathPrefix("/").Handler(http.StripPrefix("/", NewClientHandler()))

	return storeHandler
}

func (s *StoreWebServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	searchTerm := r.URL.Query().Get("searchTerm")
	if "" == searchTerm {
		http.Error(
			w,
			"no search term specified (use URL query parameter `searchTerm`)",
			400,
		)
		return
	}

	// go through all the buckets and all revisions
	searchResults, err := s.store.Search(searchTerm)
	if nil != err {
		http.Error(
			w,
			fmt.Sprintf("couldn't search store. Error: %s", err),
			500,
		)
		return
	}

	if 0 == len(searchResults) {
		searchResults = []*intelligentstore.SearchResult{}
	}

	render.JSON(w, r, searchResults)
}
