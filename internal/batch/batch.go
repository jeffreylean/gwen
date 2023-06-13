package batch

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jeffreylean/gwen/internal/record"
	"github.com/jeffreylean/gwen/internal/sink"
	"github.com/jeffreylean/gwen/internal/sink/bigquery"
)

type Batch struct {
	// Data hold in this batch
	data []record.Record
	// Pointer to point at last avaible position
	// Initial size window for the batch
	size uint64
	// Inital time window interval in milliseconds
	interval time.Duration
	// Start time of the time window
	windowStart time.Time
	// Channel to trigger readiness of batch
	ready chan bool
	// Mutex
	mutex   sync.Mutex
	writers []sink.Writer
}

func New(size uint64, interval time.Duration) (*Batch, error) {
	// Add writers
	bqWriters, err := bigquery.New("airasia-tracking-stg", "eventlog", "atomic_events")
	if err != nil {
		return nil, err
	}
	writers := []sink.Writer{bqWriters}
	return &Batch{size: size, interval: interval, data: make([]record.Record, 0), windowStart: time.Now(), ready: make(chan bool), mutex: sync.Mutex{}, writers: writers}, nil
}

// Append incoming data to the batch,
// it might the trigger point of Arrow serialization for size sliding window
func (b *Batch) Append(data []byte) {
	// Before we append it to the batch, we need to check the window
	b.mutex.Lock()
	b.data = append(b.data, data)
	b.mutex.Unlock()
	if len(b.data)+1 > int(b.size) {
		b.ready <- true
	}
}

func (b *Batch) Run(ctx context.Context) {
	// Initiate time window monitoring goroutine
	go func(ctx context.Context) {
		ticker := time.NewTicker(b.interval)
		for {
			select {
			case <-ticker.C:
				b.mutex.Lock()
				b.ready <- true
				b.mutex.Unlock()
			case <-ctx.Done():
				log.Println("Stoping batch window...")
				return
			}
		}
	}(ctx)

	// Initiate process goroutine
	go func(ctx context.Context) {
		for {
			select {
			case <-b.ready:
				log.Println("Processing..")
				// TODO: data processing
				b.process()
				log.Println("Reset pointer..")
				// Clear the data
				// TODO: can optimize this part, keep the allocated memory and reuse
				// create a pointer to keep track of the array
				log.Println("Length of data", len(b.data))
				b.data = nil
			case <-ctx.Done():
				log.Println("Stoping batch processor...")
				return
			}
		}
	}(ctx)
}

func (b *Batch) process() {
	for _, writer := range b.writers {
		if err := writer.Write(b.data); err != nil {
			log.Println("error while writing to sink:", err)
		}
	}
}
