// Package: log
// File: partition_io.go
// Core Work: Partition write path with automatic segment rollover, and read path with segment lookup
// Exported: none (methods on Partition: Append, ReadAt)
package log

/*
Functions Manifest & Usage:
- (p *Partition) Append(rec *Record) (uint64, error): Appends record to active segment, rotates on rollover, notifies flusher.
- (p *Partition) rotateActiveSegmentLocked() error: Seals current active segment and creates a new active head segment.
- (p *Partition) ReadAt(targetOffset uint64) (*Record, error): Reads record at targetOffset by locating owning segment without blocking fsync.
- (p *Partition) findSegmentLocked(targetOffset uint64) *Segment: Binary searches segments list to locate segment holding targetOffset.
*/

import (
	"errors"
	"fmt"
	"os"
	"io"
)

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
