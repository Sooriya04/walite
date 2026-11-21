package log

import (
	"testing"
	"time"
)

func TestPartition_RetentionZeroModTimePreserved(t *testing.T) {
	tempDir := t.TempDir()
	cfg := PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    100,
		IndexIntervalBytes: 32,
		Cleaner: CleanerConfig{
			MaxRetentionAge: 1 * time.Millisecond,
		},
	}

	p, err := OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed open partition: %v", err)
	}
	defer p.Close()

	// Append enough records to create multiple segments
	for i := 0; i < 6; i++ {
		_, err := p.Append(&Record{Value: []byte("retention-test-data-payload")})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	if p.SegmentsCount() < 2 {
		t.Fatalf("expected at least 2 segments, got %d", p.SegmentsCount())
	}

	// Close the logFile of segment 0 so ModTime() returns zero-time
	p.mu.Lock()
	oldest := p.segments[0]
	_ = oldest.logFile.Close()
	oldest.logFile = nil
	p.mu.Unlock()

	if !oldest.ModTime().IsZero() {
		t.Fatalf("expected zero ModTime when logFile is nil, got %v", oldest.ModTime())
	}

	// Wait past MaxRetentionAge
	time.Sleep(10 * time.Millisecond)

	// CleanSegments must NOT prune oldest segment because its ModTime is zero
	pruned, err := p.CleanSegments()
	if err != nil {
		t.Fatalf("CleanSegments failed: %v", err)
	}
	if pruned != 0 {
		t.Fatalf("expected 0 pruned segments when modTime is zero, got %d", pruned)
	}
}

func TestPartition_RotateEmptySegmentGuard(t *testing.T) {
	tempDir := t.TempDir()
	cfg := PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    200,
		IndexIntervalBytes: 64,
	}

	p, err := OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed open partition: %v", err)
	}
	defer p.Close()

	p.mu.Lock()
	defer p.mu.Unlock()

	// Calling rotateActiveSegmentLocked on empty segment must return error
	if err := p.rotateActiveSegmentLocked(); err == nil {
		t.Fatalf("expected error rotating empty active segment, got nil")
	}
}
