// Package: test
// File: cleaner_test.go
// Core Work: Segment retention verification, byte-limit GC, age-based eviction, and watermark truncation
// Exported: TestCleaner_*
package test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/barbaravane/walite/internal/log"
)

func TestCleaner_TruncateBefore(t *testing.T) {
	tempDir := t.TempDir()
	// Segment rollover after ~3 records (header 28 + val 12 = 40B; 3 records = 120B)
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    120,
		IndexIntervalBytes: 32,
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed to open partition: %v", err)
	}
	defer p.Close()

	// Append 15 records -> creates ~5 segments (base 0, 3, 6, 9, 12)
	for i := 0; i < 15; i++ {
		rec := &log.Record{Value: []byte(fmt.Sprintf("val-%08d", i))}
		if _, err := p.Append(rec); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	initialSegments := p.SegmentsCount()
	if initialSegments < 4 {
		t.Fatalf("expected at least 4 segments, got %d", initialSegments)
	}

	if p.OldestOffset() != 0 {
		t.Fatalf("expected initial oldest offset 0, got %d", p.OldestOffset())
	}

	// Truncate sealed segments whose NextOffset <= 6 (removes base 0 and 3)
	pruned, err := p.TruncateBefore(6)
	if err != nil {
		t.Fatalf("truncate failed: %v", err)
	}
	if pruned == 0 {
		t.Fatalf("expected at least 1 pruned segment, got %d", pruned)
	}

	if p.OldestOffset() < 6 {
		t.Fatalf("expected oldest offset >= 6, got %d", p.OldestOffset())
	}

	// Reading pruned offset 0 must return ErrOffsetOutOfRange
	_, err = p.ReadAt(0)
	if !errors.Is(err, log.ErrOffsetOutOfRange) {
		t.Fatalf("expected ErrOffsetOutOfRange for pruned offset 0, got %v", err)
	}

	// Reading available offset 14 must succeed
	rec, err := p.ReadAt(14)
	if err != nil {
		t.Fatalf("read at offset 14 failed: %v", err)
	}
	expectedVal := []byte(fmt.Sprintf("val-%08d", 14))
	if !bytes.Equal(rec.Value, expectedVal) {
		t.Fatalf("mismatched value at offset 14: got %s, want %s", rec.Value, expectedVal)
	}

	// Verify pruned files were removed from disk
	base0Log := filepath.Join(tempDir, log.FormatSegmentFileName(0, ".log"))
	if _, err := os.Stat(base0Log); !os.IsNotExist(err) {
		t.Fatalf("expected base 0 log file to be deleted from disk, but it exists")
	}

	// Active segment preservation: even TruncateBefore(999999) must keep active segment
	_, err = p.TruncateBefore(999999)
	if err != nil {
		t.Fatalf("truncate all failed: %v", err)
	}
	if p.SegmentsCount() != 1 {
		t.Fatalf("expected active segment to remain (count 1), got %d", p.SegmentsCount())
	}

	// Able to continue appending to active segment
	newOffset, err := p.Append(&log.Record{Value: []byte("active-alive")})
	if err != nil {
		t.Fatalf("append after full truncate failed: %v", err)
	}
	if newOffset != 15 {
		t.Fatalf("expected new offset 15, got %d", newOffset)
	}
}

func TestCleaner_RetentionByBytes(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    100,
		IndexIntervalBytes: 32,
		Cleaner: log.CleanerConfig{
			MaxPartitionBytes: 250, // Keep total bytes under ~250
		},
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed to open partition: %v", err)
	}
	defer p.Close()

	// Append records to accumulate ~500+ bytes across 5+ segments
	for i := 0; i < 15; i++ {
		_, err := p.Append(&log.Record{Value: []byte(fmt.Sprintf("retention-byte-%04d", i))})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	if p.TotalBytes() <= 250 {
		t.Fatalf("expected total bytes > 250 before GC, got %d", p.TotalBytes())
	}

	pruned, err := p.CleanSegments()
	if err != nil {
		t.Fatalf("clean segments failed: %v", err)
	}
	if pruned == 0 {
		t.Fatalf("expected pruned segments > 0, got %d", pruned)
	}

	// Verify total bytes is reduced or only active segment remains
	if p.TotalBytes() > 250 && p.SegmentsCount() > 1 {
		t.Fatalf("expected total bytes <= 250 or single active segment, got bytes %d, segs %d",
			p.TotalBytes(), p.SegmentsCount())
	}
}

func TestCleaner_RetentionByAge(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    100,
		IndexIntervalBytes: 32,
		Cleaner: log.CleanerConfig{
			MaxRetentionAge: 20 * time.Millisecond,
		},
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed to open partition: %v", err)
	}
	defer p.Close()

	// Create historical segments
	for i := 0; i < 8; i++ {
		_, err := p.Append(&log.Record{Value: []byte(fmt.Sprintf("age-val-%d", i))})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	initialSegs := p.SegmentsCount()
	if initialSegs < 2 {
		t.Fatalf("expected multiple segments, got %d", initialSegs)
	}

	// Wait for segments to age past MaxRetentionAge
	time.Sleep(35 * time.Millisecond)

	// Append one more record to create a fresh active segment that is new
	_, err = p.Append(&log.Record{Value: []byte("fresh-active")})
	if err != nil {
		t.Fatalf("append fresh failed: %v", err)
	}

	pruned, err := p.CleanSegments()
	if err != nil {
		t.Fatalf("clean segments failed: %v", err)
	}
	if pruned == 0 {
		t.Fatalf("expected aged segments to be pruned, got %d", pruned)
	}
}

func TestCleaner_BackgroundTicker(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    80,
		IndexIntervalBytes: 32,
		Cleaner: log.CleanerConfig{
			MaxPartitionBytes: 150,
			CleanInterval:     25 * time.Millisecond,
		},
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed to open partition: %v", err)
	}
	defer p.Close()

	// Generate 12 records across multiple segments
	for i := 0; i < 12; i++ {
		_, err := p.Append(&log.Record{Value: []byte(fmt.Sprintf("ticker-%d", i))})
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	// Wait for background cleaner ticker to prune
	deadline := time.Now().Add(300 * time.Millisecond)
	cleaned := false
	for time.Now().Before(deadline) {
		if p.TotalBytes() <= 200 || p.SegmentsCount() <= 2 {
			cleaned = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !cleaned {
		t.Fatalf("background cleaner did not prune partition within deadline; totalBytes: %d, segs: %d",
			p.TotalBytes(), p.SegmentsCount())
	}
}

func TestCleaner_StandaloneLifecycle(t *testing.T) {
	var cleanCallCount int64
	cleanFn := func() (int, error) {
		atomic.AddInt64(&cleanCallCount, 1)
		return 1, nil
	}

	cfg := log.CleanerConfig{
		CleanInterval: 15 * time.Millisecond,
	}
	cleaner := log.NewCleaner(cfg, cleanFn)

	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&cleanCallCount) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if atomic.LoadInt64(&cleanCallCount) == 0 {
		t.Fatal("expected background cleaner to trigger cleanFn")
	}

	cleaner.Stop()
	cleaner.Stop() // idempotent

	var nilCleaner *log.Cleaner
	nilCleaner.Stop() // nil safety
}
