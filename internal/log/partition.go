// Package: log
// File: partition.go
// Core Work: Partition lifecycle, multi-segment directory bootstrap, and resource closure
// Exported: ErrOffsetOutOfRange, ErrPartitionClosed, ErrRecordExceedsSegmentSize, PartitionConfig, Partition, OpenPartition
package log

/*
Functions Manifest & Usage:
- OpenPartition(cfg PartitionConfig) (*Partition, error): Bootstraps or opens a partition directory with historical segments and flusher/cleaner.
- (p *Partition) bootstrapSegments() error: Discovers, sorts, opens existing segment files on disk, or creates active segment 0.
- (p *Partition) Sync() error: Syncs all partition segments to disk and resets flusher unflushed counter.
- (p *Partition) Close() error: Closes flusher, cleaner, and all segment file descriptors under lock.
*/

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	// ErrOffsetOutOfRange is returned when a read request targets an offset outside the partition's bounds.
	ErrOffsetOutOfRange = errors.New("walite: requested offset is out of range")

	// ErrRecordExceedsSegmentSize is returned when a record's encoded size exceeds MaxSegmentBytes.
	ErrRecordExceedsSegmentSize = errors.New("walite: record size exceeds maximum segment capacity")
)

// PartitionConfig specifies directory paths, rollover thresholds, durability flush policies, and retention policies.
type PartitionConfig struct {
	Directory          string
	MaxSegmentBytes    uint64
	IndexIntervalBytes uint32
	Flush              FlusherConfig
	Cleaner            CleanerConfig
}

// Partition manages an active write-head segment and a list of sealed historical segments.
type Partition struct {
	mu       sync.RWMutex
	dir      string
	cfg      PartitionConfig
	segments []*Segment
	active   *Segment
	flusher  *Flusher
	cleaner  *Cleaner
	closed   bool
}

// OpenPartition initializes or recovers a partition at cfg.Directory.
// It scans for existing segment files, sorts them by baseOffset, recovers them,
// and initializes a fresh active segment if none exist.
func OpenPartition(cfg PartitionConfig) (*Partition, error) {
	if cfg.MaxSegmentBytes == 0 {
		cfg.MaxSegmentBytes = MaxSegmentBytes
	}
	if cfg.IndexIntervalBytes == 0 {
		cfg.IndexIntervalBytes = DefaultIndexIntervalBytes
	}

	if err := os.MkdirAll(cfg.Directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create partition directory: %w", err)
	}

	p := &Partition{
		dir: cfg.Directory,
		cfg: cfg,
	}

	if err := p.bootstrapSegments(); err != nil {
		_ = p.Close()
		return nil, err
	}

	p.flusher = NewFlusher(cfg.Flush, p.Sync)
	p.cleaner = NewCleaner(cfg.Cleaner, p.CleanSegments)

	return p, nil
}

func (p *Partition) bootstrapSegments() error {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return fmt.Errorf("failed to read partition directory: %w", err)
	}

	baseOffsetsMap := make(map[uint64]bool)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".log") {
			baseStr := strings.TrimSuffix(name, ".log")
			baseOffset, err := strconv.ParseUint(baseStr, 10, 64)
			if err == nil {
				baseOffsetsMap[baseOffset] = true
			}
		}
	}

	var baseOffsets []uint64
	for base := range baseOffsetsMap {
		baseOffsets = append(baseOffsets, base)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for _, base := range baseOffsets {
		segCfg := SegmentConfig{
			BaseOffset:         base,
			MaxBytes:           p.cfg.MaxSegmentBytes,
			IndexIntervalBytes: p.cfg.IndexIntervalBytes,
			Directory:          p.dir,
		}
		seg, err := OpenSegment(segCfg)
		if err != nil {
			return fmt.Errorf("failed to open segment at base %d: %w", base, err)
		}
		p.segments = append(p.segments, seg)
	}

	if len(p.segments) == 0 {
		segCfg := SegmentConfig{
			BaseOffset:         0,
			MaxBytes:           p.cfg.MaxSegmentBytes,
			IndexIntervalBytes: p.cfg.IndexIntervalBytes,
			Directory:          p.dir,
		}
		seg, err := OpenSegment(segCfg)
		if err != nil {
			return fmt.Errorf("failed to initialize active segment: %w", err)
		}
		p.segments = append(p.segments, seg)
	}

	p.active = p.segments[len(p.segments)-1]
	return nil
}

func (p *Partition) Sync() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return os.ErrClosed
	}

	active := p.active
	if p.flusher != nil {
		p.flusher.ResetUnflushed()
	}
	p.mu.Unlock()

	if active != nil {
		return active.Sync()
	}
	return nil
}

func (p *Partition) Close() error {
	if p.cleaner != nil {
		p.cleaner.Stop()
	}
	if p.flusher != nil {
		p.flusher.Stop()
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true

	var firstErr error
	for _, seg := range p.segments {
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
