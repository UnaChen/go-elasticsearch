// Licensed to Elasticsearch B.V. under one or more agreements.
// Elasticsearch B.V. licenses this file to you under the Apache 2.0 License.
// See the LICENSE file in the project root for more information.

// +build !integration

package esutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/estransport"
)

var defaultRoundTripFunc = func(*http.Request) (*http.Response, error) {
	return &http.Response{Body: ioutil.NopCloser(strings.NewReader(`{}`))}, nil
}

type mockTransport struct {
	RoundTripFunc func(*http.Request) (*http.Response, error)
}

func (t *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.RoundTripFunc == nil {
		return defaultRoundTripFunc(req)
	}
	return t.RoundTripFunc(req)
}

func TestBulkIndexer(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		var (
			wg sync.WaitGroup

			countReqs int
			testfile  string
			numItems  = 6
		)

		es, _ := elasticsearch.NewClient(elasticsearch.Config{Transport: &mockTransport{
			RoundTripFunc: func(*http.Request) (*http.Response, error) {
				countReqs++
				switch countReqs {
				case 1:
					testfile = "testdata/bulk_response_1a.json"
				case 2:
					testfile = "testdata/bulk_response_1b.json"
				case 3:
					testfile = "testdata/bulk_response_1c.json"
				}
				bodyContent, _ := ioutil.ReadFile(testfile)
				return &http.Response{Body: ioutil.NopCloser(bytes.NewBuffer(bodyContent))}, nil
			},
		}})

		var stats BulkIndexerRetryStats

		cfg := BulkIndexerConfig{
			FlushBytes: 50,
			Client:     es,
			OnFlushRetry: func(ctx context.Context, s BulkIndexerRetryStats, err error) (time.Duration, bool, error) {
				stats.NumAdded += s.NumAdded
				stats.NumFailed += s.NumFailed
				return 0, false, nil
			}}
		if os.Getenv("DEBUG") != "" {
			cfg.DebugLogger = log.New(os.Stdout, "", 0)
		}

		bi, _ := NewBulkIndexer(cfg)

		for i := 1; i <= numItems; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				err := bi.Add(context.Background(), BulkIndexerItem{
					Action:     "foo",
					DocumentID: strconv.Itoa(i),
					Body:       []byte(fmt.Sprintf(`{"title":"foo-%d"}`, i)),
				})
				if err != nil {
					t.Fatalf("Unexpected error: %s", err)
				}
			}(i)
		}
		wg.Wait()

		if err := bi.Close(context.Background()); err != nil {
			t.Errorf("Unexpected error: %s", err)
		}

		// added = numitems
		if stats.NumAdded != uint64(numItems) {
			t.Errorf("Unexpected NumAdded: want=%d, got=%d", numItems, stats.NumAdded)
		}
		// failed = 1x conflict + 1x not_found
		if stats.NumFailed != 2 {
			t.Errorf("Unexpected NumFailed: want=%d, got=%d", 2, stats.NumFailed)
		}

	})

	t.Run("Add() Timeout", func(t *testing.T) {
		es, _ := elasticsearch.NewClient(elasticsearch.Config{Transport: &mockTransport{}})
		bi, _ := NewBulkIndexer(BulkIndexerConfig{Client: es})
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		time.Sleep(100 * time.Millisecond)

		var errs []error
		for i := 0; i < 10; i++ {
			errs = append(errs, bi.Add(ctx, BulkIndexerItem{Action: "delete", DocumentID: "timeout"}))
		}
		if err := bi.Close(context.Background()); err != nil {
			t.Errorf("Unexpected error: %s", err)
		}

		var gotError bool
		for _, err := range errs {
			if err != nil && err.Error() == "context deadline exceeded" {
				gotError = true
			}
		}
		if !gotError {
			t.Errorf("Expected timeout error, but none in: %q", errs)
		}
	})

	t.Run("Close() Cancel", func(t *testing.T) {
		es, _ := elasticsearch.NewClient(elasticsearch.Config{Transport: &mockTransport{}})
		bi, _ := NewBulkIndexer(BulkIndexerConfig{
			FlushBytes: 1,
			Client:     es,
		})

		for i := 0; i < 10; i++ {
			bi.Add(context.Background(), BulkIndexerItem{Action: "foo"})
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := bi.Close(ctx); err == nil {
			t.Errorf("Expected context cancelled error, but got: %v", err)
		}
	})

	t.Run("Indexer Callback", func(t *testing.T) {
		esCfg := elasticsearch.Config{
			Transport: &mockTransport{
				RoundTripFunc: func(*http.Request) (*http.Response, error) {
					return nil, fmt.Errorf("Mock transport error")
				},
			},
		}
		if os.Getenv("DEBUG") != "" {
			esCfg.Logger = &estransport.ColorLogger{
				Output:             os.Stdout,
				EnableRequestBody:  true,
				EnableResponseBody: true,
			}
		}

		es, _ := elasticsearch.NewClient(esCfg)

		var indexerError error
		biCfg := BulkIndexerConfig{
			Client: es,
			OnFlushRetry: func(ctx context.Context, stats BulkIndexerRetryStats, err error) (time.Duration, bool, error) {
				indexerError = err
				return 0, false, nil
			},
		}
		if os.Getenv("DEBUG") != "" {
			biCfg.DebugLogger = log.New(os.Stdout, "", 0)
		}

		bi, _ := NewBulkIndexer(biCfg)

		if err := bi.Add(context.Background(), BulkIndexerItem{
			Action: "foo",
		}); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		bi.Close(context.Background())

		if indexerError == nil {
			t.Errorf("Expected indexerError to not be nil")
		}
	})

	t.Run("OnFlushRetry callbacks", func(t *testing.T) {
		errRetryFailed := errors.New("retry fail")

		var stats BulkIndexerRetryStats

		es, _ := elasticsearch.NewClient(elasticsearch.Config{Transport: &mockTransport{}})
		bi, _ := NewBulkIndexer(BulkIndexerConfig{
			Client:     es,
			Index:      "foo",
			FlushBytes: 1,
			OnFlushRetry: func(ctx context.Context, s BulkIndexerRetryStats, err error) (time.Duration, bool, error) {
				stats.NumAdded += s.NumAdded
				if s.Count < 3 {
					fmt.Printf(">>> Flush retry %d: FlushBytes(%d), NumFailed(%d), NumAdded(%d), in %d ms\n", s.Count, s.FlushBytes, s.NumFailed, s.NumAdded, s.Duration)

					return 0, true, errRetryFailed
				}
				return 0, false, errRetryFailed
			},
			OnFlushRetryEnd: func(ctx context.Context, d int64, err error) {
				if err != nil && !errors.Is(err, errRetryFailed) {
					t.Errorf("Unexpected OnFlushRetryError: want=%s, got=%s", errRetryFailed, err)
				}
			},
		})

		err := bi.Add(context.Background(), BulkIndexerItem{
			Action: "index",
			Body:   []byte(`{"title":"foo"}`),
		})
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		if err := bi.Close(context.Background()); err != nil {
			t.Errorf("Unexpected error: %s", err)
		}

		if stats.NumAdded != uint64(1) {
			t.Errorf("Unexpected NumAdded: %d", stats.NumAdded)
		}
	})

	t.Run("TooManyRequests", func(t *testing.T) {
		var (
			wg sync.WaitGroup

			countReqs int
			numItems  = 2
		)

		esCfg := elasticsearch.Config{
			Transport: &mockTransport{
				RoundTripFunc: func(*http.Request) (*http.Response, error) {
					countReqs++
					if countReqs <= 4 {
						return &http.Response{
							StatusCode: http.StatusTooManyRequests,
							Status:     "429 TooManyRequests",
							Body:       ioutil.NopCloser(strings.NewReader(`{"took":1}`))}, nil
					}
					bodyContent, _ := ioutil.ReadFile("testdata/bulk_response_1c.json")
					return &http.Response{
						StatusCode: http.StatusOK,
						Status:     "200 OK",
						Body:       ioutil.NopCloser(bytes.NewBuffer(bodyContent)),
					}, nil
				},
			},

			MaxRetries:    5,
			RetryOnStatus: []int{502, 503, 504, 429},
			RetryBackoff: func(i int) time.Duration {
				if os.Getenv("DEBUG") != "" {
					fmt.Printf("*** Retry #%d\n", i)
				}
				return time.Duration(i) * 100 * time.Millisecond
			},
		}
		if os.Getenv("DEBUG") != "" {
			esCfg.Logger = &estransport.ColorLogger{Output: os.Stdout}
		}
		es, _ := elasticsearch.NewClient(esCfg)

		var stats BulkIndexerRetryStats
		biCfg := BulkIndexerConfig{
			FlushBytes: 50,
			Client:     es,
			OnFlushRetry: func(ctx context.Context, s BulkIndexerRetryStats, err error) (time.Duration, bool, error) {
				stats = s
				return 0, false, nil
			}}
		if os.Getenv("DEBUG") != "" {
			biCfg.DebugLogger = log.New(os.Stdout, "", 0)
		}

		bi, _ := NewBulkIndexer(biCfg)

		for i := 1; i <= numItems; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				err := bi.Add(context.Background(), BulkIndexerItem{
					Action: "foo",
					Body:   []byte(`{"title":"foo"}`),
				})
				if err != nil {
					t.Fatalf("Unexpected error: %s", err)
				}
			}(i)
		}
		wg.Wait()

		if err := bi.Close(context.Background()); err != nil {
			t.Errorf("Unexpected error: %s", err)
		}

		if stats.NumAdded != uint64(numItems) {
			t.Errorf("Unexpected NumAdded: want=%d, got=%d", numItems, stats.NumAdded)
		}

		if stats.NumFailed != 0 {
			t.Errorf("Unexpected NumFailed: want=%d, got=%d", 0, stats.NumFailed)
		}

	})

	t.Run("Custom JSON Decoder", func(t *testing.T) {
		var stats BulkIndexerRetryStats
		es, _ := elasticsearch.NewClient(elasticsearch.Config{Transport: &mockTransport{}})
		bi, _ := NewBulkIndexer(BulkIndexerConfig{
			Client:  es,
			Decoder: customJSONDecoder{},
			OnFlushRetry: func(ctx context.Context, s BulkIndexerRetryStats, err error) (time.Duration, bool, error) {
				stats = s
				return 0, false, nil
			}})

		err := bi.Add(context.Background(), BulkIndexerItem{
			Action:     "index",
			DocumentID: "1",
			Body:       []byte(`{"title":"foo"}`),
		})
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		if err := bi.Close(context.Background()); err != nil {
			t.Errorf("Unexpected error: %s", err)
		}

		if stats.NumAdded != uint64(1) {
			t.Errorf("Unexpected NumAdded: %d", stats.NumAdded)
		}
	})

	t.Run("Worker.writeMeta()", func(t *testing.T) {
		type args struct {
			item BulkIndexerItem
		}
		tests := []struct {
			name string
			args args
			want string
		}{
			{
				"without _index and _id",
				args{BulkIndexerItem{Action: "index"}},
				`{"index":{}}` + "\n",
			},
			{
				"with _id",
				args{BulkIndexerItem{
					Action:     "index",
					DocumentID: "42",
				}},
				`{"index":{"_id":"42"}}` + "\n",
			},
			{
				"with _index",
				args{BulkIndexerItem{
					Action: "index",
					Index:  "test",
				}},
				`{"index":{"_index":"test"}}` + "\n",
			},
			{
				"with _index and _id",
				args{BulkIndexerItem{
					Action:     "index",
					DocumentID: "42",
					Index:      "test",
				}},
				`{"index":{"_id":"42","_index":"test"}}` + "\n",
			},
		}
		for _, tt := range tests {
			tt := tt

			t.Run(tt.name, func(t *testing.T) {
				w := &bulkIndexer{
					buf: bytes.NewBuffer(make([]byte, 0, 5e+6)),
					aux: make([]byte, 0, 512),
				}
				w.writeMeta(tt.args.item)

				if w.buf.String() != tt.want {
					t.Errorf("worker.writeMeta() %s = got [%s], want [%s]", tt.name, w.buf.String(), tt.want)
				}

			})
		}
	})
}

type customJSONDecoder struct{}

func (d customJSONDecoder) UnmarshalFromReader(r io.Reader, blk *BulkIndexerResponse) error {
	return json.NewDecoder(r).Decode(blk)
}
