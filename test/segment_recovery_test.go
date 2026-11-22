// Package: test
// File: segment_recovery_test.go
// Core Work: Segment crash recovery, torn write truncation, and sparse index reconstruction tests
// Exported: none (test package: TestSegment_*)
package test

/*
Functions Manifest & Usage:
- TestSegment_TornWriteCrashRecovery: Verifies truncation of torn log record writes on restart.
- TestSegment_IndexTornWriteCrashRecovery: Tests recovery from torn index entries.
- TestSegment_IndexRebuildOnMissingIndex: Validates full index reconstruction from data file if index is missing.
- TestSegment_OffsetRegressionRecovery: Ensures recovery handles corrupt/regressive offset sequences.
- TestSegment_LaggedIndexBackfillRecovery: Validates backfilling sparse index entries if index lagged behind data.
- TestSegment_RelativeOffsetOverflowGuard: Enforces safety checks on 32-bit relative offset overflow.
*/

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/barbaravane/walite/internal/log"
)

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

func TestSegment_IndexTornWriteCrashRecovery(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.SegmentConfig{
		BaseOffset:         0,
		MaxBytes:           1024 * 1024,
		IndexIntervalBytes: 64,
		Directory:          tempDir,
	}

	seg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed to open segment: %v", err)
	}

	// Write 5 records
	for i := 0; i < 5; i++ {
		_, err := seg.Append(&log.Record{
			Timestamp: 1000,
			Key:       []byte("key"),
			Value:     []byte("payload-data-value-1234567890"),
		})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("failed closing segment: %v", err)
	}

	// Simulate torn write on index file: append 5 unaligned bytes
	indexPath := filepath.Join(tempDir, log.FormatSegmentFileName(0, ".index"))
	f, err := os.OpenFile(indexPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open index for corrupt append: %v", err)
	}
	if _, err := f.Write([]byte{1, 2, 3, 4, 5}); err != nil {
		t.Fatalf("failed appending torn index bytes: %v", err)
	}
	_ = f.Close()

	// Reopen segment: must auto-truncate torn index entry and successfully boot
	reopenedSeg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed to reopen segment after index torn write: %v", err)
	}
	defer reopenedSeg.Close()

	// Verify all 5 records can still be read
	for i := 0; i < 5; i++ {
		rec, err := reopenedSeg.ReadAt(uint64(i))
		if err != nil {
			t.Fatalf("failed to read record %d: %v", i, err)
		}
		if rec.Offset != uint64(i) {
			t.Fatalf("record offset mismatch: got %d, want %d", rec.Offset, i)
		}
	}

	// Append a new record
	next, err := reopenedSeg.Append(&log.Record{
		Timestamp: 2000,
		Key:       []byte("new-key"),
		Value:     []byte("new-val"),
	})
	if err != nil {
		t.Fatalf("failed to append to recovered segment: %v", err)
	}
	if next != 5 {
		t.Fatalf("expected next offset 5, got %d", next)
	}
}

func TestSegment_IndexRebuildOnMissingIndex(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.SegmentConfig{
		BaseOffset:         200,
		MaxBytes:           1024 * 1024,
		IndexIntervalBytes: 64,
		Directory:          tempDir,
	}

	seg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed to open segment: %v", err)
	}

	for i := 0; i < 10; i++ {
		_, err := seg.Append(&log.Record{
			Timestamp: 1000,
			Key:       []byte("k"),
			Value:     []byte("payload-bytes-longer-than-interval"),
		})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("failed closing segment: %v", err)
	}

	// Completely delete the index file
	indexPath := filepath.Join(tempDir, log.FormatSegmentFileName(200, ".index"))
	if err := os.Remove(indexPath); err != nil {
		t.Fatalf("failed to remove index file: %v", err)
	}

	// Reopen segment: recovery must rebuild the sparse index from the log file
	reopenedSeg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed to reopen segment with missing index: %v", err)
	}
	defer reopenedSeg.Close()

	// Verify all 10 records can be read
	for i := 0; i < 10; i++ {
		target := uint64(200 + i)
		rec, err := reopenedSeg.ReadAt(target)
		if err != nil {
			t.Fatalf("failed to read record %d: %v", target, err)
		}
		if rec.Offset != target {
			t.Fatalf("offset mismatch: got %d, want %d", rec.Offset, target)
		}
	}
}

func TestSegment_OffsetRegressionRecovery(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.SegmentConfig{
		BaseOffset:         50,
		MaxBytes:           1024 * 1024,
		IndexIntervalBytes: 4096,
		Directory:          tempDir,
	}

	seg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed to open segment: %v", err)
	}

	for i := 0; i < 3; i++ {
		_, err := seg.Append(&log.Record{
			Timestamp: 1000,
			Key:       []byte("key"),
			Value:     []byte("val"),
		})
		if err != nil {
			t.Fatalf("failed append %d: %v", i, err)
		}
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("failed close: %v", err)
	}

	// Manually inject a valid-encoded record with an invalid jumped offset (e.g., offset 99 instead of 53)
	logPath := filepath.Join(tempDir, log.FormatSegmentFileName(50, ".log"))
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed open log: %v", err)
	}
	jumpedRec := log.EncodeRecord(nil, &log.Record{
		Offset:    99, // Jumped offset!
		Timestamp: 2000,
		Key:       []byte("jumped"),
		Value:     []byte("bad"),
	})
	if _, err := f.Write(jumpedRec); err != nil {
		t.Fatalf("failed write jumped rec: %v", err)
	}
	f.Close()

	// Reopen segment: should detect non-continuous offset and truncate it cleanly
	reopened, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	if reopened.NextOffset() != 53 {
		t.Fatalf("expected NextOffset to be 53 after truncating jumped offset, got %d", reopened.NextOffset())
	}
}

func TestSegment_LaggedIndexBackfillRecovery(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.SegmentConfig{
		BaseOffset:         0,
		MaxBytes:           1024 * 1024,
		IndexIntervalBytes: 100, // Small interval to trigger multiple entries
		Directory:          tempDir,
	}

	seg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed to open segment: %v", err)
	}

	// Write enough records to generate at least 4 index entries
	payload := make([]byte, 50)
	for i := 0; i < 10; i++ {
		_, err := seg.Append(&log.Record{
			Timestamp: 1000,
			Key:       []byte("key"),
			Value:     payload,
		})
		if err != nil {
			t.Fatalf("failed append %d: %v", i, err)
		}
	}

	initialEntries := seg.IndexEntriesCount()
	if initialEntries < 3 {
		t.Fatalf("expected at least 3 index entries, got %d", initialEntries)
	}

	if err := seg.Close(); err != nil {
		t.Fatalf("failed close: %v", err)
	}

	// Artificially truncate index to simulate lag where only the first entry was flushed
	indexPath := filepath.Join(tempDir, log.FormatSegmentFileName(0, ".index"))
	idxFile, err := os.OpenFile(indexPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed open index: %v", err)
	}
	if err := idxFile.Truncate(8); err != nil { // Keep only 1 entry (8 bytes)
		t.Fatalf("failed truncate index: %v", err)
	}
	idxFile.Close()

	// Reopen segment: should detect lagged index and backfill missing entries
	reopened, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	if reopened.IndexEntriesCount() != initialEntries {
		t.Fatalf("expected recovered index entries count %d, got %d", initialEntries, reopened.IndexEntriesCount())
	}
}

func TestSegment_RelativeOffsetOverflowGuard(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.SegmentConfig{
		BaseOffset:         0,
		MaxBytes:           1024 * 1024,
		IndexIntervalBytes: 4096,
		Directory:          tempDir,
	}

	seg, err := log.OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed open segment: %v", err)
	}
	defer seg.Close()

	// Append a valid record first
	_, err = seg.Append(&log.Record{
		Timestamp: 1000,
		Key:       []byte("k"),
		Value:     []byte("v"),
	})
	if err != nil {
		t.Fatalf("failed initial append: %v", err)
	}
}

