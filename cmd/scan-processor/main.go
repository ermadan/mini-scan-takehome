package main

import (
	"context"
	"encoding/json"

	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/database"
	"github.com/censys/scan-takehome/pkg/scanning"
)

func main() {
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	if projectID == "" {
		log.Fatalf("PUBSUB_PROJECT_ID environment variable not set")
	}

	batchSize, err := strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if err != nil {
		log.Fatalf("Failed to read batch size: %v", err)
	}

	db, err := database.NewDatabase("/database/scans.db")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// testing code, see README
	testingIp := os.Getenv("TEST_ORDER")
	if testingIp != "" {
		scan := scanning.Scan{
			Ip:          testingIp,
			Port:        8080,
			Service:     "HTTP",
			Timestamp:   math.MaxUint32,
			DataVersion: scanning.V2,
			Data:        &scanning.V2Data{ResponseStr: "TESTTEST"},
		}
		db.StartBatch()
		db.WriteScan(&scan)
		db.FinishBatch()
		log.Printf("Testing ip configured, %s", testingIp)
	}

	flush := func(messages []*pubsub.Message) error {
		start := time.Now()
		if err := db.StartBatch(); err != nil {
			return err
		}
		for _, msg := range messages {
			var scan scanning.Scan
			if err := json.Unmarshal(msg.Data, &scan); err != nil {
				log.Printf("Failed to unmarshal message: %v", err)
				//TODO: add to dead letter queue
			} else {
				// testing code, see README
				if testingIp != "" {
					if testingIp == scan.Ip {
						scan.Port = 8080
						scan.Service = "HTTP"
						log.Printf("!!!!!!!!Attempt to update testing ip: %v", scan)
					}
				}
				if err := db.WriteScan(&scan); err != nil {
					return err
				}
			}
		}
		err = db.FinishBatch()
		log.Printf("flushed: %d, %v", len(messages), time.Since(start))
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	sub := client.Subscription("scan-sub")

	messages := make([]*pubsub.Message, 0, batchSize)
	var mutex sync.Mutex

	go func() {
		err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			// The actual message processing (unmarshalling) is heppening during the batch processing.
			// It can be moved out but will require additional memory to keep unmarshalled scans as long as
			// messages for ack/nack after batch processing
			// The batch processing is wrapped in lock to avoid race condition when pubsub is
			// ivoking the callback, making application songle-threaded, this is again - payoff for
			// not having memory doubled.
			mutex.Lock()
			messages = append(messages, msg)
			// The issue with this approach is that with delayed message previous will be waiting for the batch to fill up.
			// Adding time-triggered flushes will solve this problem
			if len(messages) >= batchSize {
				if err := flush(messages); err != nil {
					log.Fatal("Flushing failed", err)
					// TODO: add circuit breaker for continous flush failures
					for _, msg := range messages {
						msg.Nack()
					}
				} else {
					for _, msg := range messages {
						msg.Ack()
					}
				}
				messages = messages[:0]
			}
			mutex.Unlock()
		})
		if err != nil {
			log.Fatalf("Failed to receive messages: %v", err)
		}
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan

	log.Println("Termination signal received, shutting down gracefully...")
	cancel() // Cancel the context to stop receiving messages
	flush(messages)
	if err != nil {
		log.Fatalf("Failed to flush: %v", err)
	}
	// testing code, see README
	if testingIp != "" {
		db.PrintAllScans()
	}
}
