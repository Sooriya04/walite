// Package: test
// File: segment_test.go
// Core Work: Segment log append, read, boundary conditions, concurrency, and benchmark tests
// Exported: none (test package: TestSegment_*, BenchmarkSegment_*)
package test

/*
Functions Manifest & Usage:
- TestSegment_AppendAndReadAt: Tests record appends and offset-addressed reads.
- TestSegment_RolloverLimit: Tests segment capacity rollover limits.
- TestSegment_MaxBytesLimit: Enforces 4GB index boundary configuration check.
- TestSegment_AppendPayloadLimit: Validates payload size limits.
- TestSegment_OffsetPrecedesBase: Validates offset monotonic progression.
- TestSegment_NextSegmentConfig: Tests derived next segment config calculation.
- TestSegment_ConcurrentAppendAndTailRead: Stresses concurrent writer with tail reader.
- TestSegment_NestedDirectoryBootstrap: Tests segment initialization in nested directory paths.
- TestSegment_ReadAtFastSkip: Tests sparse index binary search skip optimization.
- TestSegment_RemoveAfterCloseDeletesIndexFile: Verifies clean unlinking of .log and .index files after close.
- BenchmarkSegment_Append: Measures append throughput and heap allocation profile.
*/

import (
	"bytes"
	"io"
	"sync/atomic"
	"time"
	"path/filepath"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/barbaravane/walite/internal/log"
)

func TestSegment_AppendAndReadAt(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.SegmentConfig{
		BaseOffset:         100,
		MaxBytes:           1024 * 1024,
		IndexIntervalBytes: 128,
		Directory:          tempDir,
	}

	seg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed to open segment: %v", err)
	}
	defer seg.Close()

	if seg.BaseOffset() != 100 {
		t.Fatalf("expected base offset 100, got %d", seg.BaseOffset())
	}
	if seg.NextOffset() != 100 {
		t.Fatalf("expected next offset 100, got %d", seg.NextOffset())
	}

	// Append 10 records
	for i := 0; i < 10; i++ {
		rec := &log.Record{
			Timestamp: time.Now().UnixNano(),
			Key:       []byte("test-key"),
			Value:     []byte("test-payload-data"),
		}
		offset, err := seg.Append(rec)
		if err != nil {
			t.Fatalf("failed to append record %d: %v", i, err)
		}
		expectedOffset := uint64(100 + i)
		if offset != expectedOffset {
			t.Fatalf("expected offset %d, got %d", expectedOffset, offset)
		}
	}

	if seg.NextOffset() != 110 {
		t.Fatalf("expected next offset 110, got %d", seg.NextOffset())
	}

	// Read each record back via ReadAt
	for i := 0; i < 10; i++ {
		targetOffset := uint64(100 + i)
		rec, err := seg.ReadAt(targetOffset)
		if err != nil {
			t.Fatalf("failed to read record at offset %d: %v", targetOffset, err)
		}
		if rec.Offset != targetOffset {
			t.Fatalf("record offset mismatch: got %d, want %d", rec.Offset, targetOffset)
		}
		if !bytes.Equal(rec.Key, []byte("test-key")) {
			t.Fatalf("record key mismatch: got %s, want test-key", rec.Key)
		}
		if !bytes.Equal(rec.Value, []byte("test-payload-data")) {
			t.Fatalf("record value mismatch: got %s, want test-payload-data", rec.Value)
		}
	}

	// Out of bounds read check
	if _, err := seg.ReadAt(99); err != log.ErrOffsetPrecedesBase {
		t.Fatalf("expected ErrOffsetPrecedesBase reading before baseOffset, got %v", err)
	}
	if _, err := seg.ReadAt(110); err != io.EOF {
		t.Fatalf("expected EOF reading at nextOffset, got %v", err)
	}
}

func TestSegment_RolloverLimit(t *testing.T) {
	tempDir := t.TempDir()
	// Each record: 28B header + 1B key + 40B val = 69B.
	// Two records = 138B. MaxBytes = 138 means segment is exactly filled.
	cfg := log.SegmentConfig{
		BaseOffset:         0,
		MaxBytes:           138,
		IndexIntervalBytes: 32,
		Directory:          tempDir,
	}

	seg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed to open segment: %v", err)
	}
	defer seg.Close()

	rec := &log.Record{
		Timestamp: 1000,
		Key:       []byte("k"),
		Value:     bytes.Repeat([]byte("v"), 40),
	}

	// Append 1: 69 bytes
	if _, err := seg.Append(rec); err != nil {
		t.Fatalf("failed first append: %v", err)
	}
	if seg.IsFull() {
		t.Fatalf("segment should not be full yet")
	}

	// Append 2: 138 bytes total
	if _, err := seg.Append(rec); err != nil {
		t.Fatalf("failed second append: %v", err)
	}
	if !seg.IsFull() {
		t.Fatalf("expected IsFull() to return true after reaching MaxBytes")
	}

	// Append 3: would breach 138 bytes -> ErrSegmentFull
	_, err = seg.Append(rec)
	if err != log.ErrSegmentFull {
		t.Fatalf("expected ErrSegmentFull on third append, got %v", err)
	}
}

func TestSegment_MaxBytesLimit(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.SegmentConfig{
		BaseOffset: 0,
		MaxBytes:   log.MaxSegmentBytes + 1,
		Directory:  tempDir,
	}

	_, err := log.OpenSegment(cfg)
	if err != log.ErrSegmentTooLarge {
		t.Fatalf("expected ErrSegmentTooLarge for MaxBytes > 4GB, got %v", err)
	}
}

func TestSegment_AppendPayloadLimit(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.SegmentConfig{
		BaseOffset: 0,
		MaxBytes:   1024 * 1024,
		Directory:  tempDir,
	}

	seg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed to open segment: %v", err)
	}
	defer seg.Close()

	rec := &log.Record{
		Key:   make([]byte, 1024),
		Value: make([]byte, log.MaxRecordPayload),
	}

	_, err = seg.Append(rec)
	if err != log.ErrRecordTooLarge {
		t.Fatalf("expected ErrRecordTooLarge, got %v", err)
	}
}

func TestSegment_OffsetPrecedesBase(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.SegmentConfig{
		BaseOffset:         500,
		MaxBytes:           1024 * 1024,
		IndexIntervalBytes: 128,
		Directory:          tempDir,
	}

	seg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed to open segment: %v", err)
	}
	defer seg.Close()

	// Explicitly specifying an offset lower than baseOffset must fail
	rec := &log.Record{
		Offset:    499,
		Timestamp: 1000,
		Key:       []byte("k"),
		Value:     []byte("v"),
	}

	_, err = seg.Append(rec)
	if err != log.ErrOffsetPrecedesBase {
		t.Fatalf("expected ErrOffsetPrecedesBase on Append, got %v", err)
	}

	// Reading an offset preceding baseOffset must return ErrOffsetPrecedesBase instead of io.EOF
	_, err = seg.ReadAt(499)
	if err != log.ErrOffsetPrecedesBase {
		t.Fatalf("expected ErrOffsetPrecedesBase on ReadAt, got %v", err)
	}

	// Reading an offset beyond nextOffset must still return io.EOF
	_, err = seg.ReadAt(500)
	if err != io.EOF {
		t.Fatalf("expected io.EOF on ReadAt beyond nextOffset, got %v", err)
	}
}

func TestSegment_NextSegmentConfig(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.SegmentConfig{
		BaseOffset:         100,
		MaxBytes:           1024 * 1024,
		IndexIntervalBytes: 128,
		Directory:          tempDir,
	}

	seg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed to open segment: %v", err)
	}
	defer seg.Close()

	for i := 0; i < 5; i++ {
		_, err := seg.Append(&log.Record{
			Timestamp: 1000,
			Key:       []byte("k"),
			Value:     []byte("v"),
		})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	nextCfg := seg.NextSegmentConfig(2*1024*1024, 256)
	if nextCfg.BaseOffset != 105 {
		t.Fatalf("expected next base offset 105, got %d", nextCfg.BaseOffset)
	}
	if nextCfg.MaxBytes != 2*1024*1024 {
		t.Fatalf("expected next MaxBytes 2MB, got %d", nextCfg.MaxBytes)
	}
	if nextCfg.IndexIntervalBytes != 256 {
		t.Fatalf("expected next IndexIntervalBytes 256, got %d", nextCfg.IndexIntervalBytes)
	}
}

func TestSegment_ConcurrentAppendAndTailRead(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.SegmentConfig{
		BaseOffset:         0,
		MaxBytes:           10 * 1024 * 1024,
		IndexIntervalBytes: 128,
		Directory:          tempDir,
	}

	seg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed to open segment: %v", err)
	}
	defer seg.Close()

	const numRecords = 500
	const numReaders = 4

	var wg sync.WaitGroup
	var stopReaders int32

	// Start concurrent readers tailing the segment
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			var currentOffset uint64 = 0
			for atomic.LoadInt32(&stopReaders) == 0 || currentOffset < numRecords {
				rec, err := seg.ReadAt(currentOffset)
				if err == io.EOF {
					time.Sleep(1 * time.Millisecond)
					continue
				}
				if err != nil {
					t.Errorf("reader %d failed reading offset %d: %v", readerID, currentOffset, err)
					return
				}
				if rec.Offset != currentOffset {
					t.Errorf("reader %d offset mismatch: got %d, want %d", readerID, rec.Offset, currentOffset)
					return
				}
				currentOffset++
			}
		}(r)
	}

	// Producer appends records concurrently
	for i := 0; i < numRecords; i++ {
		rec := &log.Record{
			Timestamp: time.Now().UnixNano(),
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("concurrent-payload-value-%d", i)),
		}
		_, err := seg.Append(rec)
		if err != nil {
			t.Fatalf("failed appending record %d: %v", i, err)
		}
	}

	atomic.StoreInt32(&stopReaders, 1)
	wg.Wait()
}

func TestSegment_NestedDirectoryBootstrap(t *testing.T) {
	tempDir := t.TempDir()
	nestedDir := filepath.Join(tempDir, "topics", "orders", "partition-0")

	cfg := log.SegmentConfig{
		BaseOffset:         100,
		MaxBytes:           1024 * 1024,
		IndexIntervalBytes: 4096,
		Directory:          nestedDir,
	}

	seg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("expected OpenSegment to create nested directory successfully, got %v", err)
	}
	defer seg.Close()

	offset, err := seg.Append(&log.Record{
		Timestamp: 1000,
		Key:       []byte("key"),
		Value:     []byte("val"),
	})
	if err != nil {
		t.Fatalf("failed append in bootstrapped directory: %v", err)
	}
	if offset != 100 {
		t.Fatalf("expected offset 100, got %d", offset)
	}
}

func TestSegment_ReadAtFastSkip(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.SegmentConfig{
		BaseOffset:         500,
		MaxBytes:           1024 * 1024,
		IndexIntervalBytes: 1024, // Sparse index interval > record size to ensure multiple records between index entries
		Directory:          tempDir,
	}

	seg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed to open segment: %v", err)
	}
	defer seg.Close()

	// Append 30 records of varied payload sizes
	records := make([]*log.Record, 30)
	for i := 0; i < 30; i++ {
		records[i] = &log.Record{
			Timestamp: int64(1000 + i),
			Key:       []byte(fmt.Sprintf("key-%03d", i)),
			Value:     bytes.Repeat([]byte{byte(i + 1)}, (i+1)*16),
		}
		offset, err := seg.Append(records[i])
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if offset != uint64(500+i) {
			t.Fatalf("expected offset %d, got %d", 500+i, offset)
		}
	}

	// Read selective offsets: first, intermediate (skipping several), last
	targets := []uint64{500, 501, 507, 515, 529}
	for _, target := range targets {
		rec, err := seg.ReadAt(target)
		if err != nil {
			t.Fatalf("ReadAt(%d) failed: %v", target, err)
		}
		idx := int(target - 500)
		if rec.Offset != target {
			t.Fatalf("expected offset %d, got %d", target, rec.Offset)
		}
		if !bytes.Equal(rec.Key, records[idx].Key) {
			t.Fatalf("key mismatch for %d", target)
		}
		if !bytes.Equal(rec.Value, records[idx].Value) {
			t.Fatalf("value mismatch for %d", target)
		}
	}

	// Non-existent offset past end returns EOF
	if _, err := seg.ReadAt(530); err != io.EOF {
		t.Fatalf("expected EOF for offset 530, got %v", err)
	}
}

func TestSegment_RemoveAfterCloseDeletesIndexFile(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.SegmentConfig{
		BaseOffset:         0,
		MaxBytes:           1024,
		IndexIntervalBytes: 32,
		Directory:          tempDir,
	}

	seg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed open segment: %v", err)
	}

	// Write record to ensure index file has content
	if _, err := seg.Append(&log.Record{Value: []byte("sample-data")}); err != nil {
		t.Fatalf("failed append: %v", err)
	}

	logPath := filepath.Join(tempDir, log.FormatSegmentFileName(0, ".log"))
	indexPath := filepath.Join(tempDir, log.FormatSegmentFileName(0, ".index"))

	// Ensure files exist before close
	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("expected log file to exist before close: %v", err)
	}
	if _, err := os.Stat(indexPath); err != nil {
		t.Fatalf("expected index file to exist before close: %v", err)
	}

	// Close the segment first (which closes s.index and sets s.index.file = nil)
	if err := seg.Close(); err != nil {
		t.Fatalf("failed close: %v", err)
	}

	// Remove must still delete both .log AND .index files from disk without orphaning
	if err := seg.Remove(); err != nil {
		t.Fatalf("failed remove after close: %v", err)
	}

	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Fatalf("expected log file to be removed after Remove(), but still exists")
	}
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		t.Fatalf("expected index file to be removed after Remove(), but orphaned index file remains")
	}
}

func BenchmarkSegment_Append(b *testing.B) {
	tempDir := b.TempDir()
	cfg := log.SegmentConfig{
		BaseOffset:         0,
		MaxBytes:           1024 * 1024 * 1024,
		IndexIntervalBytes: 4096,
		Directory:          tempDir,
	}
	seg, err := log.OpenSegment(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer seg.Close()

	rec := &log.Record{
		Key:   []byte("bench-key"),
		Value: []byte("bench-value-payload-1234567890"),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rec.Timestamp = int64(i)
		_, err := seg.Append(rec)
		if err != nil {
			b.Fatal(err)
		}
	}
}
