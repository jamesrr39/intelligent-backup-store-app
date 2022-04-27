package storewebserver

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/logpkg"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/dal"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore/intelligentstore"
)

// StoreWebServer represents a handler handling requests for a store.
type StoreWebServer struct {
	store *dal.IntelligentStoreDAL
	http.Handler
}

// NewStoreWebServer creates a StoreWebServer and sets up the routing for the services it provides.
func NewStoreWebServer(logger *logpkg.Logger, store *dal.IntelligentStoreDAL) (*StoreWebServer, errorsx.Error) {
	staticFilesHandler, err := NewClientHandler()
	if err != nil {
		return nil, err
	}

	router := chi.NewRouter()
	router.Use(middleware.Logger)
	storeHandler := &StoreWebServer{store, router}

	router.Get("/api/search", storeHandler.handleSearch)

	router.Mount("/api/buckets/", NewBucketService(logger, store))
	router.Mount("/", staticFilesHandler)

	return storeHandler, nil
}

func (s *StoreWebServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	searchTerm := r.URL.Query().Get("searchTerm")
	if searchTerm == "" {
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

	if len(searchResults) == 0 {
		searchResults = []*intelligentstore.SearchResult{}
	}

	render.JSON(w, r, searchResults)
}
