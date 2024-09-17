package stats

import (
	"io"
)

// ReaderWrapper wraps an io.Reader and calls OnDone with the total number of bytes read from the underlying reader
// as an argument.
// OnDone is called at most once when the underlying reader returns the first error, that can be EOF, or when it get
// Close()d, whatever happens first.
// If the underlying reader also implements io.Closer, calling Close() will also call Close() on it. Otherwise,
// the Close() operation always returns nil.
type ReaderWrapper struct {
	Underlying io.Reader
	OnDone     func(totalRead uint64)

	read     uint64
	reported bool
}

func (w *ReaderWrapper) Read(p []byte) (n int, err error) {
	n, err = w.Underlying.Read(p)
	w.read += uint64(n)

	if err != nil {
		w.report()
	}

	return
}

func (w *ReaderWrapper) Close() (err error) {
	if closer, ok := w.Underlying.(io.Closer); ok {
		err = closer.Close()
	}

	w.report()
	return
}

func (w *ReaderWrapper) report() {
	if w.reported {
		return
	}

	w.reported = true
	w.OnDone(w.read)
}
