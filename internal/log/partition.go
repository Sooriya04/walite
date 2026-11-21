// Package: log
// File: partition.go
// Core Work: Multi-segment partition management, active segment rollover, and partition-wide offset reads
// Exported: PartitionConfig, Partition, OpenPartition, ErrOffsetOutOfRange
package log

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	// ErrOffsetOutOfRange is returned when a read request targets an offset outside the partition's bounds.
	ErrOffsetOutOfRange = errors.New("walite: requested offset is out of range")
)

// PartitionConfig specifies directory paths and rollover thresholds for a partition.
type PartitionConfig struct {
	Directory          string
	MaxSegmentBytes    uint64
	IndexIntervalBytes uint32
}

// Partition manages an active write-head segment and a list of sealed historical segments.
type Partition struct {
	mu       sync.RWMutex
	dir      string
	cfg      PartitionConfig
	segments []*Segment
	active   *Segment
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

// Append appends a record to the partition. If the active segment is full,
// it rotates to a new segment automatically and appends the record.
func (p *Partition) Append(rec *Record) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, os.ErrClosed
	}

	offset, err := p.active.Append(rec)
	if err == nil {
		return offset, nil
	}

	if errors.Is(err, ErrSegmentFull) {
		if err := p.rotateActiveSegmentLocked(); err != nil {
			return 0, fmt.Errorf("failed to rotate segment: %w", err)
		}
		// Retry append on new active segment
		return p.active.Append(rec)
	}

	return 0, err
}

func (p *Partition) rotateActiveSegmentLocked() error {
	if err := p.active.Sync(); err != nil {
		return fmt.Errorf("failed to sync active segment before rollover: %w", err)
	}

	nextCfg := p.active.NextSegmentConfig(p.cfg.MaxSegmentBytes, p.cfg.IndexIntervalBytes)
	newSeg, err := OpenSegment(nextCfg)
	if err != nil {
		return fmt.Errorf("failed to open rotated segment: %w", err)
	}

	p.segments = append(p.segments, newSeg)
	p.active = newSeg
	return nil
}

// ReadAt reads a record at the specified absolute offset across partition segments.
func (p *Partition) ReadAt(targetOffset uint64) (*Record, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, os.ErrClosed
	}

	seg := p.findSegmentLocked(targetOffset)
	p.mu.RUnlock()

	if seg == nil {
		return nil, ErrOffsetOutOfRange
	}

	rec, err := seg.ReadAt(targetOffset)
	if err != nil {
		if errors.Is(err, ErrOffsetPrecedesBase) || errors.Is(err, io.EOF) {
			return nil, ErrOffsetOutOfRange
		}
		return nil, err
	}
	return rec, nil
}

// findSegmentLocked locates the segment that owns targetOffset using binary search.
func (p *Partition) findSegmentLocked(targetOffset uint64) *Segment {
	n := len(p.segments)
	if n == 0 {
		return nil
	}

	if targetOffset < p.segments[0].BaseOffset() {
		return nil
	}

	low := 0
	high := n - 1
	bestIdx := -1

	for low <= high {
		mid := low + (high-low)/2
		if p.segments[mid].BaseOffset() <= targetOffset {
			bestIdx = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if bestIdx == -1 {
		return nil
	}
	return p.segments[bestIdx]
}

// NextOffset returns the next monotonic offset that will be assigned.
func (p *Partition) NextOffset() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.active == nil {
		return 0
	}
	return p.active.NextOffset()
}

// SegmentsCount returns the total number of segments in the partition.
func (p *Partition) SegmentsCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.segments)
}

// Sync flushes data in all segments to disk.
func (p *Partition) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return os.ErrClosed
	}

	for _, seg := range p.segments {
		if err := seg.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Close flushes and closes all underlying segments.
func (p *Partition) Close() error {
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
