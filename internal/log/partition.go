// Package: log
// File: partition.go
// Core Work: Multi-segment partition management, active segment rollover, and partition-wide offset reads
// Exported: PartitionConfig, Partition, OpenPartition, ErrOffsetOutOfRange, ErrRecordExceedsSegmentSize
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
	"time"
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

// Append appends a record to the partition. If the active segment is full,
// it rotates to a new segment automatically and appends the record.
func (p *Partition) Append(rec *Record) (uint64, error) {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return 0, os.ErrClosed
	}

	encodedLen := uint64(HeaderSize + len(rec.Key) + len(rec.Value))
	if encodedLen > p.cfg.MaxSegmentBytes {
		p.mu.Unlock()
		return 0, ErrRecordExceedsSegmentSize
	}

	offset, err := p.active.Append(rec)
	if errors.Is(err, ErrSegmentFull) {
		if p.active.CurrentSize() == 0 {
			p.mu.Unlock()
			return 0, ErrSegmentFull
		}
		if err := p.rotateActiveSegmentLocked(); err != nil {
			p.mu.Unlock()
			return 0, fmt.Errorf("failed to rotate segment: %w", err)
		}
		// Retry append on new active segment
		offset, err = p.active.Append(rec)
	}
	if err != nil {
		p.mu.Unlock()
		return 0, err
	}

	var activeToSync *Segment
	if p.flusher != nil {
		if p.flusher.OnAppend(encodedLen) {
			activeToSync = p.active
			p.flusher.ResetUnflushed()
		}
	}

	p.mu.Unlock()

	// Execute disk fsync outside of partition-wide mutex lock so readers across
	// all historical segments and active segment tail are not blocked during slow disk I/O.
	if activeToSync != nil {
		if err := activeToSync.Sync(); err != nil {
			return 0, fmt.Errorf("failed to sync active segment: %w", err)
		}
	}

	return offset, nil
}

func (p *Partition) rotateActiveSegmentLocked() error {
	if p.active.CurrentSize() == 0 {
		return errors.New("walite: cannot rotate empty active segment")
	}

	if err := p.active.Sync(); err != nil {
		return fmt.Errorf("failed to sync active segment before rollover: %w", err)
	}
	if p.flusher != nil {
		p.flusher.ResetUnflushed()
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
		p.mu.RLock()
		closed := p.closed
		p.mu.RUnlock()
		if !closed && errors.Is(err, os.ErrClosed) {
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

// Flusher returns the durability flusher instance attached to the partition.
func (p *Partition) Flusher() *Flusher {
	return p.flusher
}

// Sync flushes data in all segments to disk and resets unflushed counts.
func (p *Partition) Sync() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return os.ErrClosed
	}

	segs := make([]*Segment, len(p.segments))
	copy(segs, p.segments)
	if p.flusher != nil {
		p.flusher.ResetUnflushed()
	}
	p.mu.Unlock()

	for _, seg := range segs {
		if err := seg.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Cleaner returns the retention cleaner instance attached to the partition.
func (p *Partition) Cleaner() *Cleaner {
	return p.cleaner
}

// OldestOffset returns the lowest base offset currently available in the partition.
func (p *Partition) OldestOffset() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.segments) == 0 {
		return 0
	}
	return p.segments[0].BaseOffset()
}

// TotalBytes returns the sum of current log data sizes across all partition segments.
func (p *Partition) TotalBytes() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	total := uint64(0)
	for _, seg := range p.segments {
		total += seg.CurrentSize()
	}
	return total
}

// TruncateBefore permanently deletes sealed historical segments whose next offset is less than
// or equal to targetOffset. The active head segment is never deleted.
func (p *Partition) TruncateBefore(targetOffset uint64) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, os.ErrClosed
	}

	deletedCount := 0
	for len(p.segments) > 1 {
		oldest := p.segments[0]
		if oldest.NextOffset() <= targetOffset {
			if err := oldest.Remove(); err != nil {
				return deletedCount, fmt.Errorf("failed to remove segment at base %d: %w", oldest.BaseOffset(), err)
			}
			p.segments[0] = nil
			p.segments = p.segments[1:]
			deletedCount++
		} else {
			break
		}
	}

	return deletedCount, nil
}

// CleanSegments applies configured retention policies (MaxRetentionAge and MaxPartitionBytes)
// to purge eligible historical segments. The active head segment is never deleted.
func (p *Partition) CleanSegments() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, os.ErrClosed
	}

	deletedCount := 0

	// 1. Time-based retention
	if p.cfg.Cleaner.MaxRetentionAge > 0 {
		cutoff := time.Now().Add(-p.cfg.Cleaner.MaxRetentionAge)
		for len(p.segments) > 1 {
			oldest := p.segments[0]
			modTime := oldest.ModTime()
			if !modTime.IsZero() && modTime.Before(cutoff) {
				if err := oldest.Remove(); err != nil {
					return deletedCount, fmt.Errorf("failed to remove aged segment at base %d: %w", oldest.BaseOffset(), err)
				}
				p.segments[0] = nil
				p.segments = p.segments[1:]
				deletedCount++
			} else {
				break
			}
		}
	}

	// 2. Size-based retention
	if p.cfg.Cleaner.MaxPartitionBytes > 0 {
		totalBytes := uint64(0)
		for _, seg := range p.segments {
			totalBytes += seg.CurrentSize()
		}

		for len(p.segments) > 1 && totalBytes > p.cfg.Cleaner.MaxPartitionBytes {
			oldest := p.segments[0]
			segSize := oldest.CurrentSize()
			if err := oldest.Remove(); err != nil {
				return deletedCount, fmt.Errorf("failed to remove oversized segment at base %d: %w", oldest.BaseOffset(), err)
			}
			p.segments[0] = nil
			p.segments = p.segments[1:]
			totalBytes -= segSize
			deletedCount++
		}
	}

	return deletedCount, nil
}

// Close flushes and closes all underlying segments and stops background flusher and cleaner.
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
