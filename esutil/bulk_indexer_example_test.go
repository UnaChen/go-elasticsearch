// Licensed to Elasticsearch B.V. under one or more agreements.
// Elasticsearch B.V. licenses this file to you under the Apache 2.0 License.
// See the LICENSE file in the project root for more information.

// +build !integration

package esutil_test

import (
	"context"
	"log"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
)

func ExampleNewBulkIndexer() {
	log.SetFlags(0)

	// Create the Elasticsearch client
	//
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		// Retry on 429 TooManyRequests statuses
		//
		RetryOnStatus: []int{502, 503, 504, 429},

		// A simple incremental backoff function
		//
		RetryBackoff: func(i int) time.Duration { return time.Duration(i) * 100 * time.Millisecond },

		// Retry up to 5 attempts
		//
		MaxRetries: 5,
	})
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	// Create the indexer
	//
	var stats esutil.BulkIndexerRetryStats
	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:     es,     // The Elasticsearch client
		Index:      "test", // The default index name
		FlushBytes: 5e+6,   // The flush threshold in bytes (default: 5M)
		OnFlushRetry: func(ctx context.Context, s esutil.BulkIndexerRetryStats, err error) (time.Duration, bool, error) {
			stats = s
			return 0, false, nil
		},
	})
	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}

	// Add an item to the indexer
	//
	err = indexer.Add(
		context.Background(),
		esutil.BulkIndexerItem{
			// Action field configures the operation to perform (index, create, delete, update)
			Action: "index",

			// DocumentID is the optional document ID
			DocumentID: "1",

			// Body is an `io.Reader` with the payload
			Body: []byte(`{"title":"Test"}`),
		},
	)
	if err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}

	// Close the indexer channel and flush remaining items
	//
	if err := indexer.Close(context.Background()); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}

	// Report the indexer statistics
	//
	if stats.NumFailed > 0 {
		log.Fatalf("Indexed [%d] documents with [%d] errors", stats.NumAdded, stats.NumFailed)
	} else {
		log.Printf("Successfully indexed [%d] documents", stats.NumAdded)
	}

	// For optimal performance, consider using a third-party package for JSON decoding and HTTP transport.
	//
	// For more information, examples and benchmarks, see:
	//
	// --> https://github.com/elastic/go-elasticsearch/tree/master/_examples/bulk
}
