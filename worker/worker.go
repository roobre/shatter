package worker

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"roob.re/refractor/stats"
)

var (
	ErrSlowMirror    = errors.New("slow mirror")
	ErrChannelClosed = errors.New("request channel closed")
	ErrCode          = errors.New("received non-ok status code")
)

type Worker struct {
	Name    string
	Mirror  string
	Timeout time.Duration
	Stats   *stats.Stats
	Client  *http.Client
}

type RequestResponse struct {
	Request    *http.Request
	ResponseCh chan ResponseErr
}

type ResponseErr struct {
	Response *http.Response
	Err      error
}

func (w Worker) String() string {
	return fmt.Sprintf("%s:%s", w.Name, w.Mirror)
}

func (w Worker) Work(requests chan RequestResponse) error {
	log.Debugf("Starting worker %s", w.String())

	client := w.Client
	if client == nil {
		client = http.DefaultClient
	}

	for req := range requests {
		response, err := func(*http.Request) (*http.Response, error) {
			throughput, goodPerformer := w.Stats.Stats(w.Name)
			if !goodPerformer {
				return nil, fmt.Errorf("%w: %.2fMiB/s", ErrSlowMirror, throughput/1024/1024)
			}

			httpReq, err := w.toMirror(req.Request)
			if err != nil {
				return nil, err
			}

			log.Infof("%s %s %s", w.Name, httpReq.Method, httpReq.URL)

			start := time.Now()
			response, err := client.Do(httpReq)
			if err != nil {
				return nil, err
			}

			if response.StatusCode > 400 {
				return nil, fmt.Errorf("%w: %d", ErrCode, response.StatusCode)
			}

			response.Body = &stats.ReaderWrapper{
				Underlying: response.Body,
				OnDone: func(written uint64) {
					sample := stats.Sample{
						Bytes:    written,
						Duration: time.Since(start),
					}
					log.Debugf("%s: %s", w.Name, sample.String())
					go w.Stats.Update(w.String(), sample)
				},
			}

			return response, nil
		}(req.Request)

		req.ResponseCh <- ResponseErr{
			response,
			err,
		}

		// In addition to reporting the error to the channel, break request loop as we don't trust this mirror anymore.
		if err != nil {
			return err
		}
	}

	return ErrChannelClosed
}

func (w Worker) toMirror(r *http.Request) (*http.Request, error) {
	newUrl, err := url.Parse(strings.TrimSuffix(w.Mirror, "/") + r.URL.Path)
	if err != nil {
		return nil, fmt.Errorf("building url: %w", err)
	}

	r.URL = newUrl
	return r, nil
}
