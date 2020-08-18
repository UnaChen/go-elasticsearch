// Licensed to Elasticsearch B.V. under one or more agreements.
// Elasticsearch B.V. licenses this file to you under the Apache 2.0 License.
// See the LICENSE file in the project root for more information.

package esutil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/pkg/errors"
)

// BulkIndexer represents a parallel, asynchronous, efficient indexer for Elasticsearch.
//
type BulkIndexer interface {
	// Add adds an item to the indexer. It returns an error when the item cannot be added.
	// Use the OnSuccess and OnFailure callbacks to get the operation result for the item.
	//
	// You must call the Close() method after you're done adding items.
	//
	// It is safe for concurrent use. When it's called from goroutines,
	// they must finish before the call to Close, eg. using sync.WaitGroup.
	Add(context.Context, BulkIndexerItem) error

	// Close waits until all added items are flushed and closes the indexer.
	Close(context.Context) error

	// Close with error reason immediately before Close(context.Context)
	CloseWithError(context.Context, error)
}

// BulkIndexerConfig represents configuration of the indexer.
//
type BulkIndexerConfig struct {
	FlushBytes int // The flush threshold in bytes. Defaults to 5MB.

	Client      *elasticsearch.Client   // The Elasticsearch client.
	Decoder     BulkResponseJSONDecoder // A custom JSON decoder.
	DebugLogger BulkIndexerDebugLogger  // An optional logger for debugging.

	OnFlushRetry    func(context.Context, BulkIndexerRetryStats, error) (time.Duration, bool, error) // Called when items failed after flushed
	OnFlushRetryEnd func(context.Context, int64, error)                                              // Called when the flush retry end (return duration, error).

	// Parameters of the Bulk API.
	Index               string
	ErrorTrace          bool
	FilterPath          []string
	Header              http.Header
	Human               bool
	Pipeline            string
	Pretty              bool
	Refresh             string
	Routing             string
	Source              []string
	SourceExcludes      []string
	SourceIncludes      []string
	Timeout             time.Duration
	WaitForActiveShards string
}

// BulkIndexerStats represents the indexer flush retry statistics.
//
type BulkIndexerRetryStats struct {
	Count      uint64
	FlushBytes uint64
	NumAdded   uint64
	NumFailed  uint64
	Duration   uint64 //time in milliseconds
}

// BulkIndexerItem represents an indexer item.
//
type BulkIndexerItem struct {
	Index           string
	Action          string
	DocumentID      string
	Body            []byte
	RetryOnConflict *int
}

// BulkIndexerResponse represents the Elasticsearch response.
//
type BulkIndexerResponse struct {
	Took      int                                  `json:"took"`
	HasErrors bool                                 `json:"errors"`
	Items     []map[string]BulkIndexerResponseItem `json:"items,omitempty"`
}

// BulkIndexerResponseItem represents the Elasticsearch response item.
//
type BulkIndexerResponseItem struct {
	Index      string `json:"_index"`
	DocumentID string `json:"_id"`
	Version    int64  `json:"_version"`
	Result     string `json:"result"`
	Status     int    `json:"status"`
	SeqNo      int64  `json:"_seq_no"`
	PrimTerm   int64  `json:"_primary_term"`

	Shards struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	} `json:"_shards"`

	Error struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
		Cause  struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"caused_by"`
	} `json:"error,omitempty"`
}

// BulkResponseJSONDecoder defines the interface for custom JSON decoders.
//
type BulkResponseJSONDecoder interface {
	UnmarshalFromReader(io.Reader, *BulkIndexerResponse) error
}

// BulkIndexerDebugLogger defines the interface for a debugging logger.
//
type BulkIndexerDebugLogger interface {
	Printf(string, ...interface{})
}

type bulkIndexer struct {
	done bool
	err  error

	queue chan BulkIndexerItem
	wg    sync.WaitGroup

	buf         *bytes.Buffer
	aux         []byte
	items       []BulkIndexerItem
	failedItems []BulkIndexerItem

	config BulkIndexerConfig
}

// NewBulkIndexer creates a new bulk indexer.
//
func NewBulkIndexer(cfg BulkIndexerConfig) (BulkIndexer, error) {
	if cfg.Client == nil {
		cfg.Client, _ = elasticsearch.NewDefaultClient()
	}

	if cfg.Decoder == nil {
		cfg.Decoder = defaultJSONDecoder{}
	}

	if cfg.FlushBytes == 0 {
		cfg.FlushBytes = 5e+6
	}

	if cfg.OnFlushRetry == nil {
		cfg.OnFlushRetry = func(context.Context, BulkIndexerRetryStats, error) (time.Duration, bool, error) {
			return 0, false, nil
		}
	}

	if cfg.OnFlushRetryEnd == nil {
		cfg.OnFlushRetryEnd = func(context.Context, int64, error) {}
	}

	bi := bulkIndexer{
		queue: make(chan BulkIndexerItem),
		buf:   bytes.NewBuffer(make([]byte, 0, cfg.FlushBytes)),
		aux:   make([]byte, 0, 512),

		config: cfg,
	}

	bi.start()
	return &bi, nil
}

// Add adds an item to the indexer.
//
// Adding an item after a call to Close() will panic.
//
func (bi *bulkIndexer) Add(ctx context.Context, item BulkIndexerItem) error {
	if bi.done {
		return bi.err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		bi.queue <- item
	}

	return nil
}

// CloseWithError stops all workers with customized error reason immediately,
// closes the indexer queue channel, drops unflushed items.
//
func (bi *bulkIndexer) CloseWithError(ctx context.Context, err error) {
	if bi.done {
		return
	}

	bi.err = err
	bi.done = true
	close(bi.queue)
	bi.wg.Wait()
}

// Close stops the periodic flush, closes the indexer queue channel,
// notifies the done channel and calls flush on all writers.
//
func (bi *bulkIndexer) Close(ctx context.Context) error {
	if bi.done {
		return nil
	}

	bi.done = true
	close(bi.queue)
	bi.wg.Wait()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		if bi.buf.Len() > 0 {
			start := time.Now()
			err := bi.flushRetry(ctx)
			bi.config.OnFlushRetryEnd(ctx, time.Since(start).Milliseconds(), err)
		}
	}
	return nil
}

func (bi *bulkIndexer) start() {
	bi.wg.Add(1)
	go func() {
		defer bi.wg.Done()

		ctx := context.Background()

		for item := range bi.queue {
			if bi.config.DebugLogger != nil {
				bi.config.DebugLogger.Printf("[worker] Received item [%s:%s]\n", item.Action, item.DocumentID)
			}

			bi.writeMeta(item)
			bi.writeBody(&item)

			bi.items = append(bi.items, item)

			if bi.buf.Len() >= bi.config.FlushBytes {
				start := time.Now()
				err := bi.flushRetry(ctx)
				bi.config.OnFlushRetryEnd(ctx, time.Since(start).Milliseconds(), err)
			}
		}
	}()
}

func (bi *bulkIndexer) reset() {
	bi.items = nil
	bi.failedItems = nil
	bi.buf.Reset()
}

func (bi *bulkIndexer) flushRetry(ctx context.Context) error {
	defer bi.reset()

	var count uint64
	for {
		start := time.Now()

		err := bi.flush(ctx)
		stats := BulkIndexerRetryStats{
			Count:      count,
			FlushBytes: uint64(bi.buf.Len()),
			NumAdded:   uint64(len(bi.items)),
			NumFailed:  uint64(len(bi.failedItems)),
			Duration:   uint64(time.Since(start).Milliseconds()),
		}

		wait, goahead, err := bi.config.OnFlushRetry(ctx, stats, err)
		if !goahead {
			return err
		}
		time.Sleep(wait)

		// retry for failed items
		failedItems := bi.failedItems
		bi.reset()
		for _, item := range failedItems {
			bi.writeMeta(item)
			bi.writeBody(&item)
			bi.items = append(bi.items, item)
		}
		count++
	}
}

// writeMeta formats and writes the item metadata to the buffer; it must be called under a lock.
//
func (bi *bulkIndexer) writeMeta(item BulkIndexerItem) {
	bi.buf.WriteRune('{')
	bi.aux = strconv.AppendQuote(bi.aux, item.Action)
	bi.buf.Write(bi.aux)
	bi.aux = bi.aux[:0]
	bi.buf.WriteRune(':')
	bi.buf.WriteRune('{')
	if item.DocumentID != "" {
		bi.buf.WriteString(`"_id":`)
		bi.aux = strconv.AppendQuote(bi.aux, item.DocumentID)
		bi.buf.Write(bi.aux)
		bi.aux = bi.aux[:0]
	}
	if item.Index != "" {
		if item.DocumentID != "" {
			bi.buf.WriteRune(',')
		}
		bi.buf.WriteString(`"_index":`)
		bi.aux = strconv.AppendQuote(bi.aux, item.Index)
		bi.buf.Write(bi.aux)
		bi.aux = bi.aux[:0]
	}
	bi.buf.WriteRune('}')
	bi.buf.WriteRune('}')
	bi.buf.WriteRune('\n')
}

// writeBody writes the item body to the buffer; it must be called under a lock.
//
func (bi *bulkIndexer) writeBody(item *BulkIndexerItem) {
	if item.Body != nil {
		bi.buf.Write(item.Body)
		bi.buf.WriteRune('\n')
	}
}

// flush writes out the worker buffer; it must be called under a lock.
//
func (bi *bulkIndexer) flush(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			bi.failedItems = append(bi.failedItems, bi.items...)
		}
	}()

	if bi.buf.Len() < 1 {
		if bi.config.DebugLogger != nil {
			bi.config.DebugLogger.Printf("[worker] Flush: Buffer empty\n")
		}
		return
	}

	if bi.config.DebugLogger != nil {
		bi.config.DebugLogger.Printf("[worker] Flush: %s\n", bi.buf.String())
	}

	req := esapi.BulkRequest{
		Index: bi.config.Index,
		Body:  bi.buf,

		Pipeline:            bi.config.Pipeline,
		Refresh:             bi.config.Refresh,
		Routing:             bi.config.Routing,
		Source:              bi.config.Source,
		SourceExcludes:      bi.config.SourceExcludes,
		SourceIncludes:      bi.config.SourceIncludes,
		Timeout:             bi.config.Timeout,
		WaitForActiveShards: bi.config.WaitForActiveShards,

		Pretty:     bi.config.Pretty,
		Human:      bi.config.Human,
		ErrorTrace: bi.config.ErrorTrace,
		FilterPath: bi.config.FilterPath,
		Header:     bi.config.Header,
	}
	res, err := req.Do(ctx, bi.config.Client)
	if err != nil {
		err = errors.Wrap(err, "flush")
		return
	}
	defer res.Body.Close()
	defer io.Copy(ioutil.Discard, res.Body)

	if res.IsError() {
		// TODO(karmi): Wrap error (include response struct)
		err = fmt.Errorf("flush: %s", res.String())
		return
	}

	var blk BulkIndexerResponse
	err = bi.config.Decoder.UnmarshalFromReader(res.Body, &blk)
	if err != nil {
		// TODO(karmi): Wrap error (include response struct)
		err = fmt.Errorf("flush: error parsing response body: %s", err)
		return
	}

	for i, blkItem := range blk.Items {
		var (
			info BulkIndexerResponseItem
		)
		item := bi.items[i]
		// The Elasticsearch bulk response contains an array of maps like this:
		//   [ { "index": { ... } }, { "create": { ... } }, ... ]
		// We range over the map, to set the first key and value as "op" and "info".
		for _, v := range blkItem {
			info = v
		}
		if info.Error.Type != "" || info.Status > 201 {
			bi.failedItems = append(bi.failedItems, item)
		}
	}

	return
}

type defaultJSONDecoder struct{}

func (d defaultJSONDecoder) UnmarshalFromReader(r io.Reader, blk *BulkIndexerResponse) error {
	return json.NewDecoder(r).Decode(blk)
}
