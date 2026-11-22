// Package: log
// File: partition_retention.go
// Core Work: Partition metadata inspection, background flusher/cleaner access, and segment retention truncation
// Exported: none (methods on Partition: NextOffset, SegmentsCount, Flusher, Cleaner, OldestOffset, TotalBytes, TruncateBefore, CleanSegments)
package log

/*
Functions Manifest & Usage:
- (p *Partition) NextOffset() uint64: Returns next monotonic offset to be produced.
- (p *Partition) SegmentsCount() int: Returns total number of active and sealed segments in partition.
- (p *Partition) Flusher() *Flusher: Returns durability flusher attached to partition.
- (p *Partition) Cleaner() *Cleaner: Returns retention cleaner attached to partition.
- (p *Partition) OldestOffset() uint64: Returns lowest offset available across non-pruned segments.
- (p *Partition) TotalBytes() uint64: Calculates total disk space consumed by partition segments.
- (p *Partition) TruncateBefore(targetOffset uint64) (int, error): Deletes sealed segments whose nextOffset <= targetOffset.
- (p *Partition) CleanSegments() (int, error): Applies age and size retention policies to prune eligible historical segments.
*/

import (
	"fmt"
	"os"
	"time"
)

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
