// Package: test
// File: segment_test.go
// Core Work: Validation of segment lifecycle, append, indexed lookup, rollover limits, and torn-write crash recovery
// Exported: None
package test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	if _, err := seg.ReadAt(99); err != io.EOF {
		t.Fatalf("expected EOF reading before baseOffset, got %v", err)
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

func TestSegment_TornWriteCrashRecovery(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.SegmentConfig{
		BaseOffset:         50,
		MaxBytes:           1024 * 1024,
		IndexIntervalBytes: 64,
		Directory:          tempDir,
	}

	seg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed to open segment: %v", err)
	}

	// Write 3 complete records
	for i := 0; i < 3; i++ {
		_, err := seg.Append(&log.Record{
			Timestamp: 1000,
			Key:       []byte("stable"),
			Value:     []byte("payload"),
		})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("failed closing segment: %v", err)
	}

	// Intentionally corrupt log by appending incomplete 10 bytes (torn write simulation)
	logPath := filepath.Join(tempDir, log.FormatSegmentFileName(50, ".log"))
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open log for corrupt append: %v", err)
	}
	if _, err := f.Write([]byte("tornbytes1")); err != nil {
		t.Fatalf("failed to append torn bytes: %v", err)
	}
	_ = f.Close()

	// Reopen segment: recovery must silently truncate torn bytes and retain the 3 valid records
	reopenedSeg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed to reopen segment after torn write: %v", err)
	}
	defer reopenedSeg.Close()

	if reopenedSeg.NextOffset() != 53 {
		t.Fatalf("expected nextOffset 53 after torn write recovery, got %d", reopenedSeg.NextOffset())
	}

	// Verify all 3 original records are completely intact
	for i := 0; i < 3; i++ {
		target := uint64(50 + i)
		rec, err := reopenedSeg.ReadAt(target)
		if err != nil {
			t.Fatalf("failed to read recovered record %d: %v", target, err)
		}
		if rec.Offset != target {
			t.Fatalf("recovered record offset mismatch: got %d, want %d", rec.Offset, target)
		}
	}

	// Verify we can continue appending immediately to the recovered segment
	newOffset, err := reopenedSeg.Append(&log.Record{
		Timestamp: 2000,
		Key:       []byte("fresh"),
		Value:     []byte("post-crash-payload"),
	})
	if err != nil {
		t.Fatalf("failed appending to recovered segment: %v", err)
	}
	if newOffset != 53 {
		t.Fatalf("expected new append offset 53, got %d", newOffset)
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
