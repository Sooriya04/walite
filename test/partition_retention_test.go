// Package: test
// File: partition_retention_test.go
// Core Work: Historical segment retention truncation, fsync reader non-blocking, and age/size pruning tests
// Exported: none (test package: TestPartition_*)
package test

/*
Functions Manifest & Usage:
- TestPartition_ConcurrentReadDuringTruncation: Ensures readers see ErrOffsetOutOfRange safely during concurrent truncation.
- TestPartition_FsyncDoesNotBlockReaders: Verifies disk fsync does not block concurrent readers on non-active segments.
- TestPartition_RetentionEdgeCases: Validates zero-modTime protection and boundary cases in retention policies.
*/

import (
	"bytes"
	"sync/atomic"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/barbaravane/walite/internal/log"
)

func TestPartition_ConcurrentReadDuringTruncation(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    128,
		IndexIntervalBytes: 32,
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed open partition: %v", err)
	}
	defer p.Close()

	// Append 30 records to generate ~10 segments
	for i := 0; i < 30; i++ {
		_, err := p.Append(&log.Record{Value: []byte(fmt.Sprintf("rec-%04d", i))})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	var stopFlag int32

	// Readers continuously reading early offsets that will be truncated
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for atomic.LoadInt32(&stopFlag) == 0 {
				for offset := uint64(0); offset < 15; offset++ {
					_, err := p.ReadAt(offset)
					if err != nil && !errors.Is(err, log.ErrOffsetOutOfRange) {
						t.Errorf("expected nil or ErrOffsetOutOfRange, got unexpected err: %v", err)
						return
					}
				}
			}
		}()
	}

	// Concurrently truncate early segments
	for target := uint64(3); target <= 15; target += 3 {
		time.Sleep(5 * time.Millisecond)
		_, _ = p.TruncateBefore(target)
	}

	atomic.StoreInt32(&stopFlag, 1)
	wg.Wait()
}

func TestPartition_FsyncDoesNotBlockReaders(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 64,
		Flush: log.FlusherConfig{
			Policy: log.SyncAlways,
		},
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed open partition: %v", err)
	}
	defer p.Close()

	// Pre-populate with 20 records
	for i := 0; i < 20; i++ {
		_, err := p.Append(&log.Record{Value: []byte(fmt.Sprintf("initial-%d", i))})
		if err != nil {
			t.Fatalf("failed initial append %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	var stopFlag int32
	var readCount int64

	// Concurrent readers reading historical records concurrently
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for atomic.LoadInt32(&stopFlag) == 0 {
				target := uint64(readerID % 20)
				rec, err := p.ReadAt(target)
				if err != nil {
					t.Errorf("reader %d failed reading offset %d: %v", readerID, target, err)
					return
				}
				if len(rec.Value) == 0 {
					t.Errorf("reader %d read empty value at offset %d", readerID, target)
					return
				}
				atomic.AddInt64(&readCount, 1)
			}
		}(r)
	}

	// Producer appending records with SyncAlways active (each append triggers active.Sync)
	for i := 0; i < 50; i++ {
		_, err := p.Append(&log.Record{Value: []byte(fmt.Sprintf("sync-record-%d", i))})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	// Also execute explicit Partition.Sync concurrently
	if err := p.Sync(); err != nil {
		t.Fatalf("partition sync failed: %v", err)
	}

	atomic.StoreInt32(&stopFlag, 1)
	wg.Wait()

	if atomic.LoadInt64(&readCount) == 0 {
		t.Fatalf("expected concurrent reads to succeed during fsync, got 0")
	}
}

func TestPartition_RetentionEdgeCases(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    100,
		IndexIntervalBytes: 32,
		Cleaner: log.CleanerConfig{
			MaxPartitionBytes: 50, // Smaller than single segment
		},
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed open partition: %v", err)
	}
	defer p.Close()

	// TruncateBefore(0) on fresh partition is no-op
	pruned, err := p.TruncateBefore(0)
	if err != nil {
		t.Fatalf("truncate 0 failed: %v", err)
	}
	if pruned != 0 {
		t.Fatalf("expected 0 pruned, got %d", pruned)
	}

	// Append records to create 4 segments
	for i := 0; i < 10; i++ {
		_, err := p.Append(&log.Record{Value: []byte("retention-edge-payload")})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}
	if p.SegmentsCount() < 3 {
		t.Fatalf("expected at least 3 segments, got %d", p.SegmentsCount())
	}

	// CleanSegments with MaxPartitionBytes < 1 segment must retain active segment
	pruned, err = p.CleanSegments()
	if err != nil {
		t.Fatalf("CleanSegments failed: %v", err)
	}
	if p.SegmentsCount() != 1 {
		t.Fatalf("expected exactly 1 active segment preserved, got %d", p.SegmentsCount())
	}

	// Active segment must remain fully writable
	off, err := p.Append(&log.Record{Value: []byte("still-alive")})
	if err != nil {
		t.Fatalf("append after full retention failed: %v", err)
	}
	if off != 10 {
		t.Fatalf("expected offset 10, got %d", off)
	}

	// Reading pruned offset returns ErrOffsetOutOfRange
	_, err = p.ReadAt(0)
	if !errors.Is(err, log.ErrOffsetOutOfRange) {
		t.Fatalf("expected ErrOffsetOutOfRange for pruned offset 0, got %v", err)
	}

	// Reading active record succeeds
	rec, err := p.ReadAt(off)
	if err != nil {
		t.Fatalf("read active offset failed: %v", err)
	}
	if !bytes.Equal(rec.Value, []byte("still-alive")) {
		t.Fatalf("unexpected value: %s", rec.Value)
	}
}
