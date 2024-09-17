package stats

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
)

const (
	// Requests that transfer less than minSampleBytes AND take less than maxDurationForMinBytes will not
	// be taken into account for ranking.
	minSampleBytes         = 512 << 10 // 512KiB
	maxDurationForMinBytes = 1 * time.Second

	maxSamples = 15.0
)

type Stats struct {
	Config
	sync.RWMutex
	workers    map[string]workerEntry
	lastReport time.Time
}

type Config struct {
	NumWorkers    int `yaml:"workers"`
	NumTopWorkers int `yaml:"topWorkers"`

	GoodThroughputMiBs float64 `yaml:"goodThroughputMiBs"`
}

func (c Config) WithDefaults() Config {
	if c.NumWorkers == 0 {
		c.NumWorkers = 12
	}

	if c.NumTopWorkers == 0 {
		c.NumTopWorkers = c.NumWorkers * 3 / 4
	}

	if c.GoodThroughputMiBs == 0 {
		c.GoodThroughputMiBs = 2
	}

	return c
}

type Sample struct {
	Bytes    uint64
	Duration time.Duration
}

func (s Sample) String() string {
	return fmt.Sprintf("%.2f MiB/s", s.Throughput()/1024/1024)
}

func (s Sample) Throughput() float64 {
	return float64(s.Bytes) / s.Duration.Seconds()
}

type workerEntry struct {
	samples int
	average float64
}

type namedEntry struct {
	name       string
	throughput float64
}

func New(c Config) *Stats {
	return &Stats{
		Config:  c.WithDefaults(),
		workers: map[string]workerEntry{},
	}
}

func (s *Stats) Remove(name string) {
	s.Lock()
	defer s.Unlock()

	delete(s.workers, name)
}

func (s *Stats) Update(name string, sample Sample) {
	// Samples for very few bytes are discarded, as the delta is too small to produce a meaningful throughput
	// calculation. However, if the amount of bytes is small but the transaction still took a substantial amount of
	// time, we keep it, as it is meaningfully telling us that this mirror is shit.
	if sample.Bytes < minSampleBytes && sample.Duration < maxDurationForMinBytes {
		log.Debugf(
			"Dropping sample for %s, not significant enough (%d bytes in %v)",
			name, sample.Bytes, sample.Duration,
		)
		return
	}

	log.Debugf("Recording sample of %s for %s", sample.String(), name)

	defer func() {
		go s.report()
	}()

	s.Lock()
	defer s.Unlock()

	w := s.workers[name]
	w.average = (w.average*float64(w.samples) + sample.Throughput()) / (float64(w.samples) + 1)
	w.samples++
	if w.samples > maxSamples {
		// As time passes, mirrors that performed very well in the past might stack an indefinitely large amount
		// of samples, which might bias how the mirror is performing now. To avoid this, the number of samples taken
		// into account is capped at maxSamples.
		w.samples = maxSamples
	}

	s.workers[name] = w
}

func (s *Stats) Stats(name string) (float64, bool) {
	entries := s.workerList()

	if len(entries) <= s.NumTopWorkers {
		log.Debugf("Less than %d workers ranked, cannot evict any yet", s.NumWorkers)
		return 0, true
	}

	position := slices.IndexFunc(entries, func(entry namedEntry) bool {
		return entry.name == name
	})

	if position == -1 {
		log.Debugf("Worker %s is not ranked yet", name)
		return 0, true
	}

	log.Debugf("Worker %s is in position %d/%d", name, position+1, len(s.workers))

	throughput := entries[position].throughput
	if throughput > s.GoodThroughputMiBs*1024*1024 {
		log.Debugf("Worker %s has an absolutely good throughput", name)
		return throughput, true
	}

	// We're good performers if we're among NumTopWorkers.
	return throughput, position < s.NumTopWorkers
}

func (s *Stats) report() {
	if !s.shouldReport() {
		return
	}

	list := s.workerList()
	statsStr := "Worker stats:"
	for _, worker := range list {
		statsStr += fmt.Sprintf("\n%.2fMiB/s\t%s", worker.throughput/1024/1024, worker.name)
	}
	log.Info(statsStr)
}

func (s *Stats) shouldReport() bool {
	s.Lock()
	defer s.Unlock()

	if time.Since(s.lastReport) < 10*time.Second {
		return false
	}

	s.lastReport = time.Now()
	return true
}

func (s *Stats) workerList() []namedEntry {
	s.RLock()
	defer s.RUnlock()

	entries := make([]namedEntry, 0, len(s.workers))

	for wName, entry := range s.workers {
		if entry.average == 0 {
			continue
		}

		entries = append(entries, namedEntry{
			name:       wName,
			throughput: entry.average,
		})
	}

	slices.SortFunc(entries, func(a, b namedEntry) bool {
		// Less func is inverted to sort in descending order (from best to worst throughput)
		return a.throughput > b.throughput
	})

	return entries
}
