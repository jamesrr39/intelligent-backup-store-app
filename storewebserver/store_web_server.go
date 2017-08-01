package storewebserver

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/jamesrr39/intelligent-backup-store-app/intelligentstore"
)

type StoreWebServer struct {
	store  *intelligentstore.IntelligentStore
	router *mux.Router
}

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
}
