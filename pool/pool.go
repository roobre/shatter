package pool

import (
	"context"
	"fmt"
	"net/http"

	log "github.com/sirupsen/logrus"

	"roob.re/refractor/names"
	"roob.re/refractor/provider/types"
	"roob.re/refractor/stats"
	"roob.re/refractor/worker"
)

type Pool struct {
	stats    *stats.Stats
	provider types.Provider
	namer    func() string

	mirrors  chan string
	requests chan worker.RequestResponse
}

func New(provider types.Provider, stats *stats.Stats) *Pool {
	return &Pool{
		provider: provider,
		stats:    stats,
		mirrors:  make(chan string),
		requests: make(chan worker.RequestResponse),
		namer:    names.Haiku,
	}
}

func (p *Pool) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := 0; i < p.stats.NumWorkers; i++ {
		go func() {
			p.manageWorker(ctx)
		}()
	}

	return p.feedMirrors(ctx)
}

func (p *Pool) feedMirrors(ctx context.Context) error {
	for {
		mirror, err := p.provider.Mirror()
		if err != nil {
			return fmt.Errorf("getting mirror from provider: %w", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case p.mirrors <- mirror:
			continue
		}
	}
}

func (p *Pool) Do(r *http.Request) (*http.Response, error) {
	respChan := make(chan worker.ResponseErr)

	p.requests <- worker.RequestResponse{
		Request:    r,
		ResponseCh: respChan,
	}

	re := <-respChan
	return re.Response, re.Err
}

func (p *Pool) manageWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case mirror := <-p.mirrors:
			worker := worker.Worker{
				Mirror: mirror,
				Stats:  p.stats,
				Name:   p.namer(),
			}

			log.Errorf("%s terminated: %v", worker.Name, worker.Work(p.requests))
			p.stats.Remove(worker.String())
		}
	}
}
