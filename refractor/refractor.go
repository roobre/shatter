package refractor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"roob.re/refractor/pool"
	"roob.re/refractor/stats"
)

type Refractor struct {
	Config
	Pool *pool.Pool

	buffers sync.Pool
}

type Config struct {
	ChunkSizeMiBs int           `yaml:"chunkSizeMiBs"`
	ChunkTimeout  time.Duration `yaml:"chunkTimeout"`
	Retries       int           `yaml:"retries"`
}

func (c Config) WithDefaults() Config {
	const (
		defaultChunkSize    = 4
		defaultChunkTimeout = 3 * time.Second
		defaultRetries      = 5
	)

	if c.ChunkSizeMiBs == 0 {
		c.ChunkSizeMiBs = defaultChunkSize
	}

	if c.ChunkTimeout == 0 {
		c.ChunkTimeout = defaultChunkTimeout
	}

	if c.Retries == 0 {
		c.Retries = defaultRetries
	}

	return c
}

func New(c Config, pool *pool.Pool) *Refractor {
	return &Refractor{
		Config: c.WithDefaults(),
		Pool:   pool,
		buffers: sync.Pool{
			New: func() any {
				return &bytes.Buffer{}
			},
		},
	}
}

func (rf *Refractor) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	url := r.URL.String()

	// TODO: Mirror-specific hacks should be a on a different, possibly config-driven object that wraps Refractor.

	// Archlinux hack: Mirrors return 404 for .db.sig files.
	if strings.HasSuffix(url, ".db.sig") {
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	// Archlinux quirk: .db files change very often between mirrors, splitting them is almost guaranteed to return a
	// corrupted file, so they are handled to a single mirror.
	if strings.HasSuffix(url, ".db") {
		rf.handlePlain(rw, r)
		return
	}

	// Other requests are refracted across mirrors.
	rf.handleRefracted(rw, r)
}

func (rf *Refractor) handlePlain(rw http.ResponseWriter, r *http.Request) {
	url := r.URL.String()

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Errorf("building GET request for %q: %v", url, err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	response, err := rf.retryRequest(req)
	if err != nil {
		log.Errorf("GET request for %q failed: %v", url, err)
		rw.WriteHeader(http.StatusBadGateway)
		return
	}

	defer response.Body.Close()

	for k, vs := range response.Header {
		for _, v := range vs {
			rw.Header().Add(k, v)
		}
	}

	_, err = io.Copy(rw, response.Body)
	if err != nil {
		log.Errorf("writing GET body: %v", err)
	}
}

func (rf *Refractor) handleRefracted(rw http.ResponseWriter, r *http.Request) {
	url := r.URL.String()

	headReq, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		log.Errorf("building HEAD request for %q: %v", url, err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	headResponse, err := rf.retryRequest(headReq)
	if err != nil {
		log.Errorf("HEAD request to %s did not succeed: %v", url, err)
		rw.WriteHeader(http.StatusBadGateway)
		return
	}

	for k, vs := range headResponse.Header {
		for _, v := range vs {
			rw.Header().Add(k, v)
		}
	}

	requests, err := rf.split(url, headResponse.ContentLength)
	if err != nil {
		log.Errorf("Splitting request: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	// This goroutine runs in parallel with the main request loop, writing bodies to the client and reporting back
	// errors so the main loop can stop if an error occurs.
	// This allows writing bytes to the client at the same time we are reading the next chunk.
	bodyChan := make(chan io.ReadCloser)
	errChan := make(chan error)
	go func() {
		for body := range bodyChan {
			_, err := io.Copy(rw, body)
			if err != nil {
				errChan <- err
			}
			// Always close body, even if an error occurred.
			body.Close()
		}

		// Close errChan to signal the main loop when we're done.
		close(errChan)
	}()

	func() {
		defer close(bodyChan) // Ensure bodyChan is break the loop early.

		for _, req := range requests {
			chunkResponse, err := rf.retryRequest(req)
			if err != nil {
				log.Errorf("Requesting chunk of %q: %v", url, err)
				return
			}

			select {
			case prevErr := <-errChan:
				log.Errorf("Error writing chunk of %q:", prevErr)
				return
			case bodyChan <- chunkResponse.Body:
			}
		}
	}()

	// Wait for the sending routine to finish and log the final error, if any.
	for err := range errChan {
		log.Errorf("Error writing chunk of %q:", err)
	}
}

func (rf *Refractor) split(url string, size int64) ([]*http.Request, error) {
	// Build requests
	var requests []*http.Request
	start := int64(0)
	for start < size {
		end := start + int64(rf.ChunkSizeMiBs)<<20
		if end > size {
			end = size
		}

		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("building ranged retryRequest for %q: %v", url, err)
		}

		req.Header.Add("range", fmt.Sprintf("bytes=%d-%d", start, end))
		// Prevent servers from gzipping request, as that would break ranges across servers.
		// This actually does mirrors a favor, preventing them from spending CPU cycles on compressing in-transport
		// linux packages which are already compressed.
		req.Header.Add("accept-encoding", "identity")

		requests = append(requests, req)

		start = end + 1 // Server returns [start-end], both inclusive, so next request should start on end + 1.
	}

	return requests, nil
}

func (rf *Refractor) retryRequest(r *http.Request) (*http.Response, error) {
	try := 0
	for {
		try++

		response, err := rf.request(r)
		if err != nil {
			log.Errorf("[%d/%d] Requesting %s[%s]: %v", try, rf.Retries, r.URL.Path, r.Header.Get("range"), err)
			if try < rf.Retries {
				continue
			}

			log.Errorf("Giving up on %s[%s]: %v", r.URL.Path, r.Header.Get("range"), err)
			return nil, err

		}

		return response, nil
	}
}

func (rf *Refractor) request(r *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), rf.ChunkTimeout)
	defer cancel()

	r = r.WithContext(ctx)

	expectedStatus := http.StatusOK
	if r.Header.Get("range") != "" {
		expectedStatus = http.StatusPartialContent
	}

	response, err := rf.Pool.Do(r)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	if response.StatusCode != expectedStatus {
		return nil, fmt.Errorf("got status %d, expected %d", response.StatusCode, expectedStatus)
	}

	if response.ContentLength == -1 {
		return nil, fmt.Errorf("got -1 content length for response")
	}

	// If this is a HEAD request there is no need to copy the body.
	if r.Method == http.MethodHead {
		return response, nil
	}

	buf := rf.buffers.Get().(*bytes.Buffer)
	buf.Reset()

	body := response.Body

	// Asynchronously wait for context and close body if it gets cancelled.
	go func() {
		<-ctx.Done()

		err := body.Close()
		if err != nil {
			log.Errorf("Closing body due to context timeout: %v", err)
		}
	}()

	// io.Copy will return early if the source body is cancelled above.
	n, err := io.Copy(buf, body)
	if err != nil {
		return nil, err
	}

	if n != response.ContentLength {
		return nil, fmt.Errorf("expected to read bytes %d but read %d instead", response.ContentLength, n)
	}

	response.Body = &stats.ReaderWrapper{
		Underlying: buf,
		OnDone: func(_ uint64) {
			rf.buffers.Put(buf)
		},
	}

	return response, nil
}
