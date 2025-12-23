package main

import (
	"context"
	"encoding/json"

	"log"
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

	db, err := database.NewDatabase("./scans.db")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

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
			mutex.Lock()
			messages = append(messages, msg)
			// issue with this approach is that with delayed message previous will be waiting for the batch to fill
			// adding time-triggered flushed would be a best solution but it will require mutex sync for the messages
			// slice and may reduce overall performance
			if len(messages) >= batchSize {
				if err := flush(messages); err != nil {
					log.Fatal("Flushing failed", err)
					for _, msg := range messages {
						msg.Nack()
					}
				}
				for _, msg := range messages {
					msg.Ack()
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
}
