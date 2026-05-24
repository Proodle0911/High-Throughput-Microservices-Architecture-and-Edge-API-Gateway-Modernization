package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pranav/portfolio-arch/proto/payment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	requests := flag.Int("n", 1000, "Number of requests to send")
	concurrency := flag.Int("c", 50, "Number of concurrent workers")
	target := flag.String("t", "127.0.0.1:50051", "Target address (Edge Gateway)")
	flag.Parse()

	fmt.Printf("Starting benchmark: %d requests, %d concurrency, target %s\n", *requests, *concurrency, *target)

	conn, err := grpc.Dial(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := paymentv1.NewPaymentIngressServiceClient(conn)

	var wg sync.WaitGroup
	requestChan := make(chan int, *requests)
	results := make(chan time.Duration, *requests)

	start := time.Now()

	// Start workers
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range requestChan {
				reqStart := time.Now()
				_, err := client.ProcessPayment(context.Background(), &paymentv1.ProcessPaymentRequest{
					IdempotencyKey:     uuid.New().String(),
					AccountId:          "acc_789",
					Amount:             10.0,
					Currency:           "USD",
					DestinationAccount: "acc_ledger",
				})
				if err != nil {
					fmt.Printf("Request failed: %v\n", err)
					continue
				}
				results <- time.Since(reqStart)
			}
		}()
	}

	// Feed requests
	for i := 0; i < *requests; i++ {
		requestChan <- i
	}
	close(requestChan)

	wg.Wait()
	close(results)

	totalDuration := time.Since(start)
	
	// Collect metrics
	var totalLatency time.Duration
	count := 0
	for l := range results {
		totalLatency += l
		count++
	}

	if count > 0 {
		fmt.Printf("\n--- Benchmark Results ---\n")
		fmt.Printf("Total Requests: %d\n", count)
		fmt.Printf("Total Time:     %v\n", totalDuration)
		fmt.Printf("Throughput:     %.2f requests/sec\n", float64(count)/totalDuration.Seconds())
		fmt.Printf("Average Latency: %v\n", totalLatency/time.Duration(count))
	}
}
