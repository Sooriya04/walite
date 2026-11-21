package log

import (
	"os"
	"testing"
)

func TestSegment_AppendLogRollbackOnIndexError(t *testing.T) {
	tempDir := t.TempDir()
	cfg := SegmentConfig{
		BaseOffset:         0,
		MaxBytes:           1024 * 1024,
		IndexIntervalBytes: 4096,
		Directory:          tempDir,
	}

	seg, err := OpenSegment(cfg)
	if err != nil {
		t.Fatalf("failed open segment: %v", err)
	}
	defer seg.Close()

	rec := &Record{
		Timestamp: 1000,
		Key:       []byte("key-1"),
		Value:     []byte("value-1"),
	}
	if _, err := seg.Append(rec); err != nil {
		t.Fatalf("first append failed: %v", err)
	}

	sizeBefore := seg.curBytes

	// Force close index file directly under lock to simulate write error on subsequent index write
	seg.mu.Lock()
	_ = seg.index.file.Close()
	seg.index.file = nil
	// Reset bytesSinceIdx to indexInterval so next append triggers index write
	seg.bytesSinceIdx = seg.indexInterval
	seg.mu.Unlock()

	rec2 := &Record{
		Timestamp: 2000,
		Key:       []byte("key-2"),
		Value:     []byte("value-2"),
	}
	_, err = seg.Append(rec2)
	if err == nil {
		t.Fatalf("expected error on append when index closed, got nil")
	}

	// Verify log file size was rolled back to sizeBefore
	logFileInfo, err := os.Stat(seg.logFile.Name())
	if err != nil {
		t.Fatalf("failed to stat log file: %v", err)
	}
	if logFileInfo.Size() != int64(sizeBefore) {
		t.Fatalf("expected log file size %d after rollback, got %d", sizeBefore, logFileInfo.Size())
	}
}
