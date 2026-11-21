// Package: test
// File: flusher_test.go
// Core Work: Durability policy verification, batch sync thresholds, background interval flush, and concurrent crash safety
// Exported: TestFlusher_*
package test

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/barbaravane/walite/internal/log"
)

func TestFlusher_SyncAlways(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 128,
		Flush: log.FlusherConfig{
			Policy: log.SyncAlways,
		},
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed to open partition: %v", err)
	}

	if p.Flusher().Policy() != log.SyncAlways {
		t.Fatalf("expected policy SyncAlways, got %v", p.Flusher().Policy())
	}

	for i := 0; i < 5; i++ {
		rec := &log.Record{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("val-%d", i)),
		}
		offset, err := p.Append(rec)
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if offset != uint64(i) {
			t.Fatalf("expected offset %d, got %d", i, offset)
		}
		// On SyncAlways, unflushed count resets immediately
		if p.Flusher().UnflushedMessages() != 0 {
			t.Fatalf("expected 0 unflushed messages after SyncAlways, got %d", p.Flusher().UnflushedMessages())
		}
	}

	// Abrupt simulate: don't call p.Close(), verify data is durable upon crash-recovery
	pRecovered, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed to recover partition: %v", err)
	}
	defer pRecovered.Close()

	if pRecovered.NextOffset() != 5 {
		t.Fatalf("expected recovered nextOffset 5, got %d", pRecovered.NextOffset())
	}

	for i := 0; i < 5; i++ {
		readRec, err := pRecovered.ReadAt(uint64(i))
		if err != nil {
			t.Fatalf("recovered read at %d failed: %v", i, err)
		}
		expectedKey := []byte(fmt.Sprintf("key-%d", i))
		if !bytes.Equal(readRec.Key, expectedKey) {
			t.Fatalf("mismatched key at %d: got %s, want %s", i, readRec.Key, expectedKey)
		}
	}
	_ = p.Close()
}

func TestFlusher_SyncBatchMessages(t *testing.T) {
	tempDir := t.TempDir()
	batchLimit := uint64(5)
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 128,
		Flush: log.FlusherConfig{
			Policy:        log.SyncBatch,
			BatchMessages: batchLimit,
		},
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed to open partition: %v", err)
	}
	defer p.Close()

	// Append 4 records (below batch threshold)
	for i := 0; i < 4; i++ {
		rec := &log.Record{Value: []byte(fmt.Sprintf("batch-val-%d", i))}
		if _, err := p.Append(rec); err != nil {
			t.Fatalf("append failed: %v", err)
		}
		if p.Flusher().UnflushedMessages() != uint64(i+1) {
			t.Fatalf("expected unflushed messages %d, got %d", i+1, p.Flusher().UnflushedMessages())
		}
	}

	// Append 5th record -> hits threshold -> triggers sync -> resets count
	rec5 := &log.Record{Value: []byte("batch-val-4")}
	if _, err := p.Append(rec5); err != nil {
		t.Fatalf("append 5 failed: %v", err)
	}

	if p.Flusher().UnflushedMessages() != 0 {
		t.Fatalf("expected unflushed messages 0 after hitting batch limit, got %d", p.Flusher().UnflushedMessages())
	}

	// Append 2 more records
	_, _ = p.Append(&log.Record{Value: []byte("msg-5")})
	_, _ = p.Append(&log.Record{Value: []byte("msg-6")})
	if p.Flusher().UnflushedMessages() != 2 {
		t.Fatalf("expected 2 unflushed messages, got %d", p.Flusher().UnflushedMessages())
	}

	// Explicit manual sync resets unflushed counter
	if err := p.Sync(); err != nil {
		t.Fatalf("manual sync failed: %v", err)
	}
	if p.Flusher().UnflushedMessages() != 0 {
		t.Fatalf("expected 0 unflushed messages after manual Sync(), got %d", p.Flusher().UnflushedMessages())
	}
}

func TestFlusher_SyncBatchBytes(t *testing.T) {
	tempDir := t.TempDir()
	// Set small batch bytes threshold (e.g. 100 bytes)
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 128,
		Flush: log.FlusherConfig{
			Policy:     log.SyncBatch,
			BatchBytes: 100,
		},
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed to open partition: %v", err)
	}
	defer p.Close()

	// Record with 40-byte value (HeaderSize 28 + 40 = 68 bytes)
	rec := &log.Record{Value: bytes.Repeat([]byte("a"), 40)}
	if _, err := p.Append(rec); err != nil {
		t.Fatalf("append 1 failed: %v", err)
	}
	if p.Flusher().UnflushedBytes() < 68 {
		t.Fatalf("expected >= 68 unflushed bytes, got %d", p.Flusher().UnflushedBytes())
	}

	// Second append adds another 68 bytes -> 136 bytes > 100 -> triggers sync & reset
	if _, err := p.Append(rec); err != nil {
		t.Fatalf("append 2 failed: %v", err)
	}
	if p.Flusher().UnflushedBytes() != 0 {
		t.Fatalf("expected 0 unflushed bytes after crossing byte threshold, got %d", p.Flusher().UnflushedBytes())
	}
}

func TestFlusher_SyncInterval(t *testing.T) {
	tempDir := t.TempDir()
	interval := 30 * time.Millisecond
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 128,
		Flush: log.FlusherConfig{
			Policy:   log.SyncInterval,
			Interval: interval,
		},
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed to open partition: %v", err)
	}
	defer p.Close()

	for i := 0; i < 5; i++ {
		_, err := p.Append(&log.Record{Value: []byte("interval-test")})
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	// Should initially have unflushed items
	if p.Flusher().UnflushedMessages() != 5 {
		t.Fatalf("expected 5 unflushed messages initially, got %d", p.Flusher().UnflushedMessages())
	}

	// Wait for background ticker to flush (give 3x interval margin)
	deadline := time.Now().Add(300 * time.Millisecond)
	flushed := false
	for time.Now().Before(deadline) {
		if p.Flusher().UnflushedMessages() == 0 {
			flushed = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !flushed {
		t.Fatalf("background interval flusher did not flush within timeout; remaining: %d", p.Flusher().UnflushedMessages())
	}
}

func TestFlusher_ConcurrentAppendsAndSync(t *testing.T) {
	tempDir := t.TempDir()
	cfg := log.PartitionConfig{
		Directory:          tempDir,
		MaxSegmentBytes:    10 * 1024,
		IndexIntervalBytes: 64,
		Flush: log.FlusherConfig{
			Policy:        log.SyncBatch,
			BatchMessages: 20,
		},
	}

	p, err := log.OpenPartition(cfg)
	if err != nil {
		t.Fatalf("failed to open partition: %v", err)
	}
	defer p.Close()

	const numProducers = 6
	const msgsPerProducer = 100
	var wg sync.WaitGroup
	var totalAppended uint64

	for w := 0; w < numProducers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < msgsPerProducer; i++ {
				rec := &log.Record{
					Key:   []byte(fmt.Sprintf("worker-%d", workerID)),
					Value: []byte(fmt.Sprintf("payload-%d-%d", workerID, i)),
				}
				_, err := p.Append(rec)
				if err != nil {
					t.Errorf("worker %d append %d failed: %v", workerID, i, err)
					return
				}
				atomic.AddUint64(&totalAppended, 1)
			}
		}(w)
	}

	wg.Wait()

	expectedTotal := uint64(numProducers * msgsPerProducer)
	if totalAppended != expectedTotal {
		t.Fatalf("expected %d total appended records, got %d", expectedTotal, totalAppended)
	}

	if p.NextOffset() != expectedTotal {
		t.Fatalf("expected NextOffset %d, got %d", expectedTotal, p.NextOffset())
	}
}

func TestFlusher_StandaloneLifecycle(t *testing.T) {
	var syncCallCount int64
	syncFn := func() error {
		atomic.AddInt64(&syncCallCount, 1)
		return nil
	}

	// Policy String representations
	if log.SyncNone.String() != "none" {
		t.Errorf("expected 'none', got %s", log.SyncNone.String())
	}
	if log.SyncAlways.String() != "always" {
		t.Errorf("expected 'always', got %s", log.SyncAlways.String())
	}
	if log.SyncBatch.String() != "batch" {
		t.Errorf("expected 'batch', got %s", log.SyncBatch.String())
	}
	if log.SyncInterval.String() != "interval" {
		t.Errorf("expected 'interval', got %s", log.SyncInterval.String())
	}
	if log.SyncPolicy(99).String() != "unknown(99)" {
		t.Errorf("expected 'unknown(99)', got %s", log.SyncPolicy(99).String())
	}

	// Interval flusher standalone
	cfg := log.FlusherConfig{
		Policy:   log.SyncInterval,
		Interval: 15 * time.Millisecond,
	}
	flusher := log.NewFlusher(cfg, syncFn)

	flusher.OnAppend(50)
	if !flusher.HasUnflushed() {
		t.Fatal("expected HasUnflushed to be true")
	}

	// Wait for ticker
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&syncCallCount) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if atomic.LoadInt64(&syncCallCount) == 0 {
		t.Fatal("expected syncFunc to be called by ticker")
	}

	// Stop idempotent
	flusher.Stop()
	flusher.Stop()

	// Nil safety
	var nilFlusher *log.Flusher
	if nilFlusher.OnAppend(10) {
		t.Fatal("nil flusher should return false on append")
	}
	if nilFlusher.HasUnflushed() {
		t.Fatal("nil flusher HasUnflushed should be false")
	}
	nilFlusher.ResetUnflushed()
	nilFlusher.Stop()
	if nilFlusher.Policy() != log.SyncNone {
		t.Fatal("nil flusher should return SyncNone")
	}
	if nilFlusher.UnflushedMessages() != 0 || nilFlusher.UnflushedBytes() != 0 {
		t.Fatal("nil flusher counters should be zero")
	}
}
