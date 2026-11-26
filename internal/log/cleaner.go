// Package: log
// File: cleaner.go
// Core Work: Partition segment retention policies, time-based and byte-based segment garbage collection
// Exported: CleanerConfig, Cleaner, NewCleaner
package log

import (
	"sync"
	"time"
)

// CleanerConfig defines segment retention and garbage collection policies.
type CleanerConfig struct {
	MaxRetentionAge   time.Duration // Evict sealed segments older than this duration (0 = disabled)
	MaxPartitionBytes uint64        // Evict sealed segments when partition exceeds this byte size (0 = disabled)
	CleanInterval     time.Duration // Background GC ticker interval (0 = manual CleanSegments only)
}

// Cleaner coordinates background segment retention checks.
type Cleaner struct {
	cfg       CleanerConfig
	cleanFunc func() (int, error)

	ticker   *time.Ticker
	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
}

// NewCleaner creates a segment retention cleaner.
// If cfg.CleanInterval is greater than 0, a background ticker periodically invokes cleanFunc.
func NewCleaner(cfg CleanerConfig, cleanFunc func() (int, error)) *Cleaner {
	c := &Cleaner{
		cfg:       cfg,
		cleanFunc: cleanFunc,
	}

	if cfg.CleanInterval > 0 {
		c.ticker = time.NewTicker(cfg.CleanInterval)
		c.stopCh = make(chan struct{})
		c.doneCh = make(chan struct{})
		go c.runTicker()
	}

	return c
}

func (c *Cleaner) runTicker() {
	defer close(c.doneCh)
	for {
		select {
		case <-c.stopCh:
			return
		case <-c.ticker.C:
			if c.cleanFunc != nil {
				_, _ = c.cleanFunc()
			}
		}
	}
}

// Stop terminates any active background cleaner ticker safely and idempotently.
func (c *Cleaner) Stop() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() {
		if c.ticker != nil {
			c.ticker.Stop()
			close(c.stopCh)
			<-c.doneCh
		}
	})
}

// Config returns the configured retention policy parameters.
func (c *Cleaner) Config() CleanerConfig {
	if c == nil {
		return CleanerConfig{}
	}
	return c.cfg
}
