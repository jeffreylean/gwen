package server

import (
	"log"
	"net/http"

	"github.com/jeffreylean/gwen/internal/batch"
)

type Server struct {
	http.Server
	batch *batch.Batch
}

func New(batch *batch.Batch) *Server {
	mux := http.NewServeMux()

	s := http.Server{
		Handler: mux,
		Addr:    ":8000",
	}
	server := &Server{s, batch}

	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/test", server.testAppendHandler)

	log.Printf("Listening to %s", s.Addr)
	go func() {
		log.Fatal((s.ListenAndServe()))
	}()

	return server
}

func healthHandler(r http.ResponseWriter, w *http.Request) {
	r.WriteHeader(http.StatusOK)
	r.Write([]byte("Healthy"))
}

func (s *Server) testAppendHandler(r http.ResponseWriter, w *http.Request) {
	for i := 0; i < 10; i++ {
		s.batch.Append([]byte("data"))
	}
	r.WriteHeader(http.StatusOK)
}
