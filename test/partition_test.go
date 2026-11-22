// Package: test
// File: partition_test.go
// Core Work: Multi-segment partition appends, monotonic indexing, rollover, and concurrency tests
// Exported: none (test package: TestPartition_*)
package test

/*
Functions Manifest & Usage:
- TestPartition_BasicAppendAndRead: Verifies standard partition append and indexed read functionality.
- TestPartition_AutoSegmentRollover: Tests active segment rotation when MaxBytes capacity is hit.
- TestPartition_ConcurrentAppendAndReads: Validates thread-safe multi-goroutine producers and consumers.
- TestPartition_ClosedOperations: Asserts ErrPartitionClosed on post-close partition calls.
- TestPartition_AppendOversizedRecord: Validates rejection of records exceeding MaxSegmentBytes.
- TestPartition_RolloverBoundsAndExactCapacity: Tests boundary conditions and exact capacity math on rollover.
*/

import (
	"bytes"
	"io"
	"os"
	"sync/atomic"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/barbaravane/walite/internal/log"
)

func TestPartition_BasicAppendAndRead(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 128,
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed to open partition: %v", err)
	}
	defer p.Close()

	if p.NextOffset() != 0 {
		t.Fatalf("expected initial nextOffset 0, got %d", p.NextOffset())
	}

	// Append 10 records
	for i := 0; i < 10; i++ {
		offset, err := p.Append(&log.Record{
			Timestamp: int64(1000 + i),
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("value-%d", i)),
		})
		if err != nil {
			t.Fatalf("failed append %d: %v", i, err)
		}
		if offset != uint64(i) {
			t.Fatalf("expected offset %d, got %d", i, offset)
		}
	}

	if p.NextOffset() != 10 {
		t.Fatalf("expected nextOffset 10, got %d", p.NextOffset())
	}

	// Read records back
	for i := 0; i < 10; i++ {
		rec, err := p.ReadAt(uint64(i))
		if err != nil {
			t.Fatalf("read %d failed: %v", i, err)
		}
		if rec.Offset != uint64(i) {
			t.Fatalf("expected offset %d, got %d", i, rec.Offset)
		}
		if string(rec.Key) != fmt.Sprintf("key-%d", i) {
			t.Fatalf("key mismatch at %d: got %s", i, rec.Key)
		}
	}

	// Read out-of-bounds offsets
	if _, err := p.ReadAt(10); !errors.Is(err, log.ErrOffsetOutOfRange) {
		t.Fatalf("expected ErrOffsetOutOfRange for offset 10, got %v", err)
	}
}

func TestPartition_AutoSegmentRollover(t *testing.T) {
	tempDir := t.TempDir()
	// Set MaxSegmentBytes small to trigger multiple segment rotations
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    256,
		IndexIntervalBytes: 64,
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed to open partition: %v", err)
	}

	// Append 20 records (each record is ~70 bytes, so ~256 bytes holds ~3 records per segment)
	for i := 0; i < 20; i++ {
		offset, err := p.Append(&log.Record{
			Timestamp: int64(2000 + i),
			Key:       []byte(fmt.Sprintf("k-%02d", i)),
			Value:     []byte("payload-data-for-rollover-test"),
		})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if offset != uint64(i) {
			t.Fatalf("expected offset %d, got %d", i, offset)
		}
	}

	segmentsCount := p.SegmentsCount()
	if segmentsCount < 4 {
		t.Fatalf("expected at least 4 segments after rollover, got %d", segmentsCount)
	}

	// Read across all segments seamlessly
	for i := 0; i < 20; i++ {
		rec, err := p.ReadAt(uint64(i))
		if err != nil {
			t.Fatalf("failed to read offset %d across segments: %v", i, err)
		}
		if rec.Offset != uint64(i) {
			t.Fatalf("expected offset %d, got %d", i, rec.Offset)
		}
	}

	if err := p.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Reopen partition and verify recovery across all segments
	reopened, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	if reopened.SegmentsCount() != segmentsCount {
		t.Fatalf("expected %d segments after reopen, got %d", segmentsCount, reopened.SegmentsCount())
	}
	if reopened.NextOffset() != 20 {
		t.Fatalf("expected nextOffset 20 after reopen, got %d", reopened.NextOffset())
	}

	// Append more records to reopened active segment
	offset, err := reopened.Append(&log.Record{
		Timestamp: 3000,
		Key:       []byte("k-20"),
		Value:     []byte("payload-20"),
	})
	if err != nil {
		t.Fatalf("failed append to reopened partition: %v", err)
	}
	if offset != 20 {
		t.Fatalf("expected offset 20, got %d", offset)
	}
}

func TestPartition_ConcurrentAppendAndReads(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    512,
		IndexIntervalBytes: 64,
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed open partition: %v", err)
	}
	defer p.Close()

	const numRecords = 200
	var stopFlag int32
	var wg sync.WaitGroup

	// Concurrent reader goroutines
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			var cursor uint64
			for atomic.LoadInt32(&stopFlag) == 0 {
				rec, err := p.ReadAt(cursor)
				if err != nil {
					if errors.Is(err, log.ErrOffsetOutOfRange) || errors.Is(err, io.EOF) {
						continue
					}
					t.Errorf("reader %d failed at offset %d: %v", readerID, cursor, err)
					return
				}
				if rec.Offset != cursor {
					t.Errorf("reader %d offset mismatch: got %d, want %d", readerID, rec.Offset, cursor)
					return
				}
				cursor++
			}
		}(r)
	}

	// Producer appends records
	for i := 0; i < numRecords; i++ {
		_, err := p.Append(&log.Record{
			Timestamp: int64(i),
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     bytes.Repeat([]byte{byte(i % 256)}, 40),
		})
		if err != nil {
			t.Fatalf("producer append %d failed: %v", i, err)
		}
	}

	atomic.StoreInt32(&stopFlag, 1)
	wg.Wait()

	if p.NextOffset() != numRecords {
		t.Fatalf("expected nextOffset %d, got %d", numRecords, p.NextOffset())
	}
}

func TestPartition_ClosedOperations(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 128,
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed open partition: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("failed close partition: %v", err)
	}

	// Verify operations on closed partition return os.ErrClosed
	if _, err := p.Append(&log.Record{Key: []byte("k"), Value: []byte("v")}); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected os.ErrClosed on Append, got %v", err)
	}
	if _, err := p.ReadAt(0); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected os.ErrClosed on ReadAt, got %v", err)
	}
	if err := p.Sync(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected os.ErrClosed on Sync, got %v", err)
	}
}

func TestPartition_AppendOversizedRecord(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    200,
		IndexIntervalBytes: 64,
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed open partition: %v", err)
	}
	defer p.Close()

	// Append record exceeding MaxSegmentBytes (300B payload > 200B segment size)
	oversized := &log.Record{
		Key:   []byte("big-key"),
		Value: bytes.Repeat([]byte("v"), 300),
	}
	_, err = p.Append(oversized)
	if !errors.Is(err, log.ErrRecordExceedsSegmentSize) {
		t.Fatalf("expected ErrRecordExceedsSegmentSize, got %v", err)
	}

	// Verify segment count remains 1 and active segment is not rotated into duplicate
	if p.SegmentsCount() != 1 {
		t.Fatalf("expected exactly 1 segment after oversized rejection, got %d", p.SegmentsCount())
	}

	// Normal append must still succeed cleanly at offset 0
	normal := &log.Record{
		Key:   []byte("k0"),
		Value: []byte("val-0"),
	}
	offset, err := p.Append(normal)
	if err != nil {
		t.Fatalf("failed normal append: %v", err)
	}
	if offset != 0 {
		t.Fatalf("expected offset 0, got %d", offset)
	}

	// Verify read succeeds
	rec, err := p.ReadAt(0)
	if err != nil {
		t.Fatalf("failed read at 0: %v", err)
	}
	if !bytes.Equal(rec.Value, []byte("val-0")) {
		t.Fatalf("unexpected value: %s", rec.Value)
	}
}

func TestPartition_RolloverBoundsAndExactCapacity(t *testing.T) {
	tempDir := t.TempDir()
	maxSegBytes := uint64(100)
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    maxSegBytes,
		IndexIntervalBytes: 32,
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed open partition: %v", err)
	}
	defer p.Close()

	// A record with encoded size exactly equal to MaxSegmentBytes:
	// HeaderSize = 28 bytes. Key = 0 bytes. Value = 72 bytes. Total = 100 bytes.
	exactRecord := &log.Record{
		Value: bytes.Repeat([]byte("e"), int(maxSegBytes-uint64(log.HeaderSize))),
	}
	offset0, err := p.Append(exactRecord)
	if err != nil {
		t.Fatalf("expected exact capacity record to succeed, got %v", err)
	}
	if offset0 != 0 {
		t.Fatalf("expected offset 0, got %d", offset0)
	}

	// Second append of exact capacity record triggers rollover cleanly to segment 1
	offset1, err := p.Append(exactRecord)
	if err != nil {
		t.Fatalf("expected second exact capacity record to rotate and succeed, got %v", err)
	}
	if offset1 != 1 {
		t.Fatalf("expected offset 1, got %d", offset1)
	}
	if p.SegmentsCount() != 2 {
		t.Fatalf("expected 2 segments after rollover, got %d", p.SegmentsCount())
	}

	// Attempt to append record with size MaxSegmentBytes + 1 (101 bytes)
	oversized := &log.Record{
		Value: bytes.Repeat([]byte("o"), int(maxSegBytes-uint64(log.HeaderSize)+1)),
	}
	_, err = p.Append(oversized)
	if !errors.Is(err, log.ErrRecordExceedsSegmentSize) {
		t.Fatalf("expected ErrRecordExceedsSegmentSize, got %v", err)
	}

	// Multiple consecutive oversized attempts should not mutate state
	for i := 0; i < 3; i++ {
		_, err := p.Append(oversized)
		if !errors.Is(err, log.ErrRecordExceedsSegmentSize) {
			t.Fatalf("expected ErrRecordExceedsSegmentSize, got %v", err)
		}
	}
	if p.SegmentsCount() != 2 {
		t.Fatalf("expected segment count to remain 2, got %d", p.SegmentsCount())
	}

	// Normal append succeeds cleanly at next sequential offset
	normal := &log.Record{Value: []byte("norm")}
	offset2, err := p.Append(normal)
	if err != nil {
		t.Fatalf("failed normal append: %v", err)
	}
	if offset2 != 2 {
		t.Fatalf("expected offset 2, got %d", offset2)
	}
}

