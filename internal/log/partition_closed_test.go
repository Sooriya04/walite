package log

import (
	"errors"
	"testing"
)

func TestPartition_PrunedSegmentClosedConvertsToOutOfRange(t *testing.T) {
	tempDir := t.TempDir()
	cfg := PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    100,
		IndexIntervalBytes: 32,
	}

	p, err := OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed open partition: %v", err)
	}
	defer p.Close()

	// Append records to create multiple segments
	for i := 0; i < 4; i++ {
		_, err := p.Append(&Record{Value: []byte("test-payload-data")})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	if p.SegmentsCount() < 2 {
		t.Fatalf("expected at least 2 segments, got %d", p.SegmentsCount())
	}

	// Close the first segment to simulate concurrent truncation/removal
	p.mu.Lock()
	firstSeg := p.segments[0]
	_ = firstSeg.Close()
	p.mu.Unlock()

	// ReadAt on offset owned by first segment must return ErrOffsetOutOfRange, not os.ErrClosed
	_, err = p.ReadAt(0)
	if !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("expected ErrOffsetOutOfRange when segment is closed under open partition, got %v", err)
	}
}

func TestPartition_TruncateBeforeSliceSlotZeroed(t *testing.T) {
	tempDir := t.TempDir()
	cfg := PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    100,
		IndexIntervalBytes: 32,
	}

	p, err := OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed open partition: %v", err)
	}
	defer p.Close()

	for i := 0; i < 6; i++ {
		_, err := p.Append(&Record{Value: []byte("test-payload-data")})
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	p.mu.Lock()
	origBacking := p.segments[:cap(p.segments)]
	targetOffset := p.segments[0].NextOffset()
	p.mu.Unlock()

	pruned, err := p.TruncateBefore(targetOffset)
	if err != nil {
		t.Fatalf("truncate failed: %v", err)
	}
	if pruned == 0 {
		t.Fatalf("expected at least 1 segment to be pruned")
	}

	// The previous index 0 in the original backing array should be nil to avoid memory leak
	if origBacking[0] != nil {
		t.Fatalf("expected underlying slice slot 0 to be nil after truncation, but pointer remains")
	}
}
