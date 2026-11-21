// Package: log
// File: flusher.go
// Core Work: Configurable durability flush policies, periodic background ticker, and batch commit sync
// Exported: SyncPolicy, SyncNone, SyncAlways, SyncBatch, SyncInterval, FlusherConfig, Flusher, NewFlusher
package log

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// SyncPolicy defines the disk durability synchronization strategy for log appends.
type SyncPolicy int

const (
	// SyncNone delegates flush timing to the OS page cache writeback or explicit manual Sync calls.
	SyncNone SyncPolicy = iota

	// SyncAlways calls fsync on the active segment synchronously on every Append call.
	SyncAlways

	// SyncBatch triggers an fsync when either batch messages or batch bytes threshold is crossed.
	SyncBatch

	// SyncInterval flushes buffered writes periodically via a background ticker goroutine.
	SyncInterval
)

// String returns a human-readable identifier for the synchronization policy.
func (s SyncPolicy) String() string {
	switch s {
	case SyncNone:
		return "none"
	case SyncAlways:
		return "always"
	case SyncBatch:
		return "batch"
	case SyncInterval:
		return "interval"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// FlusherConfig specifies durability policies and threshold controls.
type FlusherConfig struct {
	Policy        SyncPolicy
	BatchMessages uint64
	BatchBytes    uint64
	Interval      time.Duration
}

// Flusher tracks unflushed message counts and byte sizes, enforcing durability policies.
type Flusher struct {
	cfg            FlusherConfig
	syncFunc       func() error
	unflushedMsgs  uint64
	unflushedBytes uint64

	ticker   *time.Ticker
	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
}

// NewFlusher creates and starts a durability flusher according to cfg.
// If cfg.Policy is SyncInterval, a background ticker goroutine is spawned to periodically invoke syncFunc.
func NewFlusher(cfg FlusherConfig, syncFunc func() error) *Flusher {
	if cfg.Policy == SyncBatch {
		if cfg.BatchMessages == 0 && cfg.BatchBytes == 0 {
			cfg.BatchMessages = 1000
			cfg.BatchBytes = 64 * 1024
		}
	} else if cfg.Policy == SyncInterval {
		if cfg.Interval <= 0 {
			cfg.Interval = 50 * time.Millisecond
		}
	}

	f := &Flusher{
		cfg:      cfg,
		syncFunc: syncFunc,
	}

	if cfg.Policy == SyncInterval {
		f.ticker = time.NewTicker(cfg.Interval)
		f.stopCh = make(chan struct{})
		f.doneCh = make(chan struct{})
		go f.runTicker()
	}

	return f
}

func (f *Flusher) runTicker() {
	defer close(f.doneCh)
	for {
		select {
		case <-f.stopCh:
			return
		case <-f.ticker.C:
			if f.HasUnflushed() && f.syncFunc != nil {
				_ = f.syncFunc()
			}
		}
	}
}

// OnAppend records newly written bytes and messages, and returns true if an immediate sync is required.
func (f *Flusher) OnAppend(recordBytes uint64) bool {
	if f == nil {
		return false
	}
	msgs := atomic.AddUint64(&f.unflushedMsgs, 1)
	bytes := atomic.AddUint64(&f.unflushedBytes, recordBytes)

	switch f.cfg.Policy {
	case SyncAlways:
		return true
	case SyncBatch:
		if f.cfg.BatchMessages > 0 && msgs >= f.cfg.BatchMessages {
			return true
		}
		if f.cfg.BatchBytes > 0 && bytes >= f.cfg.BatchBytes {
			return true
		}
		return false
	default:
		return false
	}
}

// HasUnflushed returns true if there are unflushed messages or bytes recorded.
func (f *Flusher) HasUnflushed() bool {
	if f == nil {
		return false
	}
	return atomic.LoadUint64(&f.unflushedMsgs) > 0 || atomic.LoadUint64(&f.unflushedBytes) > 0
}

// ResetUnflushed resets unflushed message and byte counters to zero.
func (f *Flusher) ResetUnflushed() {
	if f == nil {
		return
	}
	atomic.StoreUint64(&f.unflushedMsgs, 0)
	atomic.StoreUint64(&f.unflushedBytes, 0)
}

// UnflushedMessages returns the current count of unflushed messages.
func (f *Flusher) UnflushedMessages() uint64 {
	if f == nil {
		return 0
	}
	return atomic.LoadUint64(&f.unflushedMsgs)
}

// UnflushedBytes returns the current byte count of unflushed data.
func (f *Flusher) UnflushedBytes() uint64 {
	if f == nil {
		return 0
	}
	return atomic.LoadUint64(&f.unflushedBytes)
}

// Policy returns the configured synchronization policy.
func (f *Flusher) Policy() SyncPolicy {
	if f == nil {
		return SyncNone
	}
	return f.cfg.Policy
}

// Stop terminates any background ticker goroutine safely and idempotently.
func (f *Flusher) Stop() {
	if f == nil {
		return
	}
	f.stopOnce.Do(func() {
		if f.ticker != nil {
			f.ticker.Stop()
			close(f.stopCh)
			<-f.doneCh
		}
	})
}
