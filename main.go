package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/jeffreylean/gwen/internal/batch"
	"github.com/jeffreylean/gwen/internal/server"
	_ "go.uber.org/automaxprocs"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the batch processor
	b, err := batch.New(10, time.Second*5)
	if err != nil {
		log.Fatal("error with initializing batch", err)
	}
	b.Run(ctx)

	// Initiate the server
	s := server.New(b)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	s.Close()
	cancel()
	// Buffer time
	time.Sleep(time.Second * 1)
	log.Println("gracefully shutting down...")
}
