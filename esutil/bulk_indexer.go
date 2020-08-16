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
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
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

	// Stats returns indexer statistics.
	Stats() BulkIndexerStats
}

// BulkIndexerConfig represents configuration of the indexer.
//
type BulkIndexerConfig struct {
	NumWorkers    int           // The number of workers. Defaults to runtime.NumCPU().
	FlushBytes    int           // The flush threshold in bytes. Defaults to 5MB.
	FlushInterval time.Duration // The flush threshold as duration. Defaults to 30sec.

	Client      *elasticsearch.Client   // The Elasticsearch client.
	Decoder     BulkResponseJSONDecoder // A custom JSON decoder.
	DebugLogger BulkIndexerDebugLogger  // An optional logger for debugging.

	OnFlushStart    func(context.Context) context.Context                                            // Called when the flush starts.
	OnFlushEnd      func(context.Context)                                                            // Called when the flush ends.
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

// BulkIndexerStats represents the indexer statistics.
//
type BulkIndexerStats struct {
	NumAdded    uint64
	NumFlushed  uint64
	NumFailed   uint64
	NumIndexed  uint64
	NumCreated  uint64
	NumUpdated  uint64
	NumDeleted  uint64
	NumRequests uint64
}

// BulkIndexerItem represents an indexer item.
//
type BulkIndexerItem struct {
	Index           string
	Action          string
	DocumentID      string
	Body            io.Reader
	RetryOnConflict *int

	OnSuccess func(context.Context, BulkIndexerItem, BulkIndexerResponseItem)        // Per item
	OnFailure func(context.Context, BulkIndexerItem, BulkIndexerResponseItem, error) // Per item
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
	wg      sync.WaitGroup
	mu      sync.Mutex
	queue   chan BulkIndexerItem
	workers []*worker
	done    bool
	err     error
	cancel  context.CancelFunc
	stats   *bulkIndexerStats

	config BulkIndexerConfig
}

type bulkIndexerStats struct {
	numAdded    uint64
	numFlushed  uint64
	numFailed   uint64
	numIndexed  uint64
	numCreated  uint64
	numUpdated  uint64
	numDeleted  uint64
	numRequests uint64
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

	if cfg.NumWorkers == 0 {
		cfg.NumWorkers = runtime.NumCPU()
	}

	if cfg.FlushBytes == 0 {
		cfg.FlushBytes = 5e+6
	}

	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 30 * time.Second
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
		config: cfg,
		stats:  &bulkIndexerStats{},
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

	atomic.AddUint64(&bi.stats.numAdded, 1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case bi.queue <- item:
	}

	return nil
}

// CloseWithError stops all workers with customized error reason immediately,
// closes the indexer queue channel, drops unflushed items.
//
func (bi *bulkIndexer) CloseWithError(ctx context.Context, err error) {
	bi.mu.Lock()
	close(bi.queue)
	bi.cancel()
	bi.wg.Wait()
	bi.err = err
	bi.done = true
	bi.mu.Unlock()
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

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		bi.wg.Wait()
	}

	for _, w := range bi.workers {
		if w.buf.Len() > 0 {
			start := time.Now()
			err := w.flushRetry(ctx)
			bi.config.OnFlushRetryEnd(ctx, time.Since(start).Milliseconds(), err)
		}
	}
	return nil
}

// Stats returns indexer statistics.
//
func (bi *bulkIndexer) Stats() BulkIndexerStats {
	return BulkIndexerStats{
		NumAdded:    atomic.LoadUint64(&bi.stats.numAdded),
		NumFlushed:  atomic.LoadUint64(&bi.stats.numFlushed),
		NumFailed:   atomic.LoadUint64(&bi.stats.numFailed),
		NumIndexed:  atomic.LoadUint64(&bi.stats.numIndexed),
		NumCreated:  atomic.LoadUint64(&bi.stats.numCreated),
		NumUpdated:  atomic.LoadUint64(&bi.stats.numUpdated),
		NumDeleted:  atomic.LoadUint64(&bi.stats.numDeleted),
		NumRequests: atomic.LoadUint64(&bi.stats.numRequests),
	}
}

// init initializes the bulk indexer.
//
func (bi *bulkIndexer) start() {
	ctx, cancel := context.WithCancel(context.Background())
	bi.cancel = cancel

	bi.queue = make(chan BulkIndexerItem, bi.config.NumWorkers)
	bi.wg.Add(bi.config.NumWorkers)
	for i := 1; i <= bi.config.NumWorkers; i++ {
		w := worker{
			id:  i,
			ch:  bi.queue,
			bi:  bi,
			buf: bytes.NewBuffer(make([]byte, 0, bi.config.FlushBytes)),
			aux: make([]byte, 0, 512)}
		w.run(ctx)
		bi.workers = append(bi.workers, &w)
	}
}

// worker represents an indexer worker.
//
type worker struct {
	id          int
	ch          <-chan BulkIndexerItem
	ticker      *time.Ticker
	bi          *bulkIndexer
	buf         *bytes.Buffer
	aux         []byte
	items       []BulkIndexerItem
	failedItems []BulkIndexerItem
}

// run launches the worker in a goroutine.
//
func (w *worker) run(ctx context.Context) {
	go func(ctx context.Context) {
		defer w.bi.wg.Done()

		if w.bi.config.DebugLogger != nil {
			w.bi.config.DebugLogger.Printf("[worker-%03d] Started\n", w.id)
		}

		ticker := time.NewTicker(w.bi.config.FlushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				if w.bi.config.DebugLogger != nil {
					w.bi.config.DebugLogger.Printf("[worker-%03d] Auto-flushing workers after %s\n", w.id, w.bi.config.FlushInterval)
				}

				if w.buf.Len() > 0 {
					start := time.Now()
					err := w.flushRetry(ctx)
					w.bi.config.OnFlushRetryEnd(ctx, time.Since(start).Milliseconds(), err)
				}

			case item, ok := <-w.ch:
				if !ok {
					return
				}

				if w.bi.config.DebugLogger != nil {
					w.bi.config.DebugLogger.Printf("[worker-%03d] Received item [%s:%s]\n", w.id, item.Action, item.DocumentID)
				}

				w.writeMeta(item)

				if err := w.writeBody(&item); err != nil {
					w.failedItems = append(w.failedItems, item)
					if item.OnFailure != nil {
						item.OnFailure(ctx, item, BulkIndexerResponseItem{}, err)
					}
					atomic.AddUint64(&w.bi.stats.numFailed, 1)
					continue
				}
				w.items = append(w.items, item)

				if w.buf.Len() >= w.bi.config.FlushBytes {
					start := time.Now()
					err := w.flushRetry(ctx)
					w.bi.config.OnFlushRetryEnd(ctx, time.Since(start).Milliseconds(), err)
				}
			}
		}

	}(ctx)
}

func (w *worker) reset() {
	w.items = nil
	w.failedItems = nil
	w.buf.Reset()

}

func (w *worker) flushRetry(ctx context.Context) error {
	defer w.reset()

	var count uint64
	for {
		start := time.Now()
		err := w.flush(ctx)
		stats := BulkIndexerRetryStats{
			Count:      count,
			FlushBytes: uint64(w.buf.Len()),
			NumAdded:   uint64(len(w.items)),
			NumFailed:  uint64(len(w.failedItems)),
			Duration:   uint64(time.Since(start).Milliseconds()),
		}

		wait, goahead, err := w.bi.config.OnFlushRetry(ctx, stats, err)
		if !goahead {
			return err
		}
		time.Sleep(wait)

		// retry for failed items
		failedItems := w.failedItems
		w.reset()
		for _, item := range failedItems {
			w.writeMeta(item)

			if err := w.writeBody(&item); err != nil {
				w.failedItems = append(w.failedItems, item)
				continue
			}
			w.items = append(w.items, item)
		}
		count++
	}
}

// writeMeta formats and writes the item metadata to the buffer; it must be called under a lock.
//
func (w *worker) writeMeta(item BulkIndexerItem) {
	w.buf.WriteRune('{')
	w.aux = strconv.AppendQuote(w.aux, item.Action)
	w.buf.Write(w.aux)
	w.aux = w.aux[:0]
	w.buf.WriteRune(':')
	w.buf.WriteRune('{')
	if item.DocumentID != "" {
		w.buf.WriteString(`"_id":`)
		w.aux = strconv.AppendQuote(w.aux, item.DocumentID)
		w.buf.Write(w.aux)
		w.aux = w.aux[:0]
	}
	if item.Index != "" {
		if item.DocumentID != "" {
			w.buf.WriteRune(',')
		}
		w.buf.WriteString(`"_index":`)
		w.aux = strconv.AppendQuote(w.aux, item.Index)
		w.buf.Write(w.aux)
		w.aux = w.aux[:0]
	}
	w.buf.WriteRune('}')
	w.buf.WriteRune('}')
	w.buf.WriteRune('\n')
}

// writeBody writes the item body to the buffer; it must be called under a lock.
//
func (w *worker) writeBody(item *BulkIndexerItem) error {
	if item.Body != nil {

		var getBody func() io.Reader

		if item.OnSuccess != nil || item.OnFailure != nil {
			var buf bytes.Buffer
			buf.ReadFrom(item.Body)
			getBody = func() io.Reader {
				r := buf
				return ioutil.NopCloser(&r)
			}
			item.Body = getBody()
		}

		if _, err := w.buf.ReadFrom(item.Body); err != nil {
			return err
		}
		w.buf.WriteRune('\n')

		if getBody != nil && (item.OnSuccess != nil || item.OnFailure != nil) {
			item.Body = getBody()
		}
	}
	return nil
}

// flush writes out the worker buffer; it must be called under a lock.
//
func (w *worker) flush(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			w.failedItems = append(w.failedItems, w.items...)
		}
	}()

	if w.bi.config.OnFlushStart != nil {
		ctx = w.bi.config.OnFlushStart(ctx)
	}

	if w.bi.config.OnFlushEnd != nil {
		defer func() { w.bi.config.OnFlushEnd(ctx) }()
	}

	if w.buf.Len() < 1 {
		if w.bi.config.DebugLogger != nil {
			w.bi.config.DebugLogger.Printf("[worker-%03d] Flush: Buffer empty\n", w.id)
		}
		return
	}

	if w.bi.config.DebugLogger != nil {
		w.bi.config.DebugLogger.Printf("[worker-%03d] Flush: %s\n", w.id, w.buf.String())
	}

	atomic.AddUint64(&w.bi.stats.numRequests, 1)
	req := esapi.BulkRequest{
		Index: w.bi.config.Index,
		Body:  w.buf,

		Pipeline:            w.bi.config.Pipeline,
		Refresh:             w.bi.config.Refresh,
		Routing:             w.bi.config.Routing,
		Source:              w.bi.config.Source,
		SourceExcludes:      w.bi.config.SourceExcludes,
		SourceIncludes:      w.bi.config.SourceIncludes,
		Timeout:             w.bi.config.Timeout,
		WaitForActiveShards: w.bi.config.WaitForActiveShards,

		Pretty:     w.bi.config.Pretty,
		Human:      w.bi.config.Human,
		ErrorTrace: w.bi.config.ErrorTrace,
		FilterPath: w.bi.config.FilterPath,
		Header:     w.bi.config.Header,
	}
	res, err := req.Do(ctx, w.bi.config.Client)
	if err != nil {
		atomic.AddUint64(&w.bi.stats.numFailed, uint64(len(w.items)))
		err = errors.Wrap(err, "flush")
		return
	}
	if res.Body != nil {
		defer res.Body.Close()
	}
	if res.IsError() {
		atomic.AddUint64(&w.bi.stats.numFailed, uint64(len(w.items)))
		// TODO(karmi): Wrap error (include response struct)
		err = fmt.Errorf("flush: %s", res.String())
		return
	}

	var blk BulkIndexerResponse
	err = w.bi.config.Decoder.UnmarshalFromReader(res.Body, &blk)
	if err != nil {
		// TODO(karmi): Wrap error (include response struct)
		err = fmt.Errorf("flush: error parsing response body: %s", err)
		return
	}

	for i, blkItem := range blk.Items {
		var (
			item BulkIndexerItem
			info BulkIndexerResponseItem
			op   string
		)
		item = w.items[i]
		// The Elasticsearch bulk response contains an array of maps like this:
		//   [ { "index": { ... } }, { "create": { ... } }, ... ]
		// We range over the map, to set the first key and value as "op" and "info".
		for k, v := range blkItem {
			op = k
			info = v
		}
		if info.Error.Type != "" || info.Status > 201 {
			atomic.AddUint64(&w.bi.stats.numFailed, 1)
			w.failedItems = append(w.failedItems, item)
			if item.OnFailure != nil {
				item.OnFailure(ctx, item, info, nil)
			}
		} else {
			atomic.AddUint64(&w.bi.stats.numFlushed, 1)
			switch op {
			case "index":
				atomic.AddUint64(&w.bi.stats.numIndexed, 1)
			case "create":
				atomic.AddUint64(&w.bi.stats.numCreated, 1)
			case "delete":
				atomic.AddUint64(&w.bi.stats.numDeleted, 1)
			case "update":
				atomic.AddUint64(&w.bi.stats.numUpdated, 1)
			}

			if item.OnSuccess != nil {
				item.OnSuccess(ctx, item, info)
			}
		}
	}

	return
}

type defaultJSONDecoder struct{}

func (d defaultJSONDecoder) UnmarshalFromReader(r io.Reader, blk *BulkIndexerResponse) error {
	return json.NewDecoder(r).Decode(blk)
}
