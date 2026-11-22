// Package: test
// File: consumer_test.go
// Core Work: Consumer offset checkpoint persistence, crash recovery replay, string bounds, and query tests
// Exported: none (test package: TestConsumer_OffsetManager*)
package test

/*
Functions Manifest & Usage:
- TestConsumer_OffsetManagerBasicCommitAndGet: Tests single checkpoint commit and lookup.
- TestConsumer_OffsetManagerBatchAndGroupHierarchy: Validates multi-checkpoint batch commits.
- TestConsumer_OffsetManagerMonotonicOverwrite: Ensures later offsets overwrite earlier offsets.
- TestConsumer_OffsetManagerCrashRestartRecovery: Verifies restart replay recovers full cache state.
- TestConsumer_OffsetManagerConcurrentCommitsAndReads: Stresses concurrent writers and readers.
- TestConsumer_OffsetManagerClosedOperations: Asserts ErrPartitionClosed when operating on closed manager.
- TestConsumer_OffsetManagerRecoveryAfterTruncation: Validates replay starts from OldestOffset after retention.
- TestConsumer_OffsetManagerStringLengthGuards: Enforces 64KB string boundaries on checkpoint fields.
- TestConsumer_OffsetManagerClosedQueries: Checks nil returns for queries on closed manager.
*/

import (
	"bytes"
	"sync/atomic"
	"time"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/barbaravane/walite/internal/consumer"
)

func TestConsumer_OffsetManagerBasicCommitAndGet(t *testing.T) {
	tempDir := t.TempDir()
	cfg := consumer.OffsetManagerConfig{
		Directory: tempDir,
	}

	om, err := consumer.OpenOffsetManager(cfg)
	if err != nil {
		t.Fatalf("failed to open offset manager: %v", err)
	}
	defer om.Close()

	// Query uncommitted target
	_, err = om.GetOffset("group-a", "orders", 0)
	if !errors.Is(err, consumer.ErrOffsetNotFound) {
		t.Fatalf("expected ErrOffsetNotFound, got %v", err)
	}

	// Commit single offset
	cp := consumer.Checkpoint{
		GroupID:     "group-a",
		Topic:       "orders",
		PartitionID: 0,
		Offset:      42,
		Metadata:    "processed-batch-1",
	}

	if err := om.CommitOffset(cp); err != nil {
		t.Fatalf("commit offset failed: %v", err)
	}

	// Get offset
	offset, err := om.GetOffset("group-a", "orders", 0)
	if err != nil {
		t.Fatalf("get offset failed: %v", err)
	}
	if offset != 42 {
		t.Fatalf("expected offset 42, got %d", offset)
	}

	// Get full checkpoint
	retrievedCP, err := om.GetCheckpoint("group-a", "orders", 0)
	if err != nil {
		t.Fatalf("get checkpoint failed: %v", err)
	}
	if retrievedCP.Offset != 42 || retrievedCP.Metadata != "processed-batch-1" {
		t.Fatalf("unexpected checkpoint data: %+v", retrievedCP)
	}
	if retrievedCP.Timestamp == 0 {
		t.Fatalf("expected non-zero timestamp")
	}
}

func TestConsumer_OffsetManagerBatchAndGroupHierarchy(t *testing.T) {
	tempDir := t.TempDir()
	cfg := consumer.OffsetManagerConfig{
		Directory: tempDir,
	}

	om, err := consumer.OpenOffsetManager(cfg)
	if err != nil {
		t.Fatalf("failed to open offset manager: %v", err)
	}
	defer om.Close()

	checkpoints := []consumer.Checkpoint{
		{GroupID: "analytics", Topic: "events", PartitionID: 0, Offset: 100},
		{GroupID: "analytics", Topic: "events", PartitionID: 1, Offset: 200},
		{GroupID: "analytics", Topic: "logs", PartitionID: 0, Offset: 50},
		{GroupID: "billing", Topic: "payments", PartitionID: 0, Offset: 10},
	}

	if err := om.CommitOffsets(checkpoints); err != nil {
		t.Fatalf("batch commit failed: %v", err)
	}

	// Verify group hierarchy
	analyticsOffsets := om.GetGroupOffsets("analytics")
	if len(analyticsOffsets) != 2 {
		t.Fatalf("expected 2 topics for analytics, got %d", len(analyticsOffsets))
	}
	if analyticsOffsets["events"][0] != 100 || analyticsOffsets["events"][1] != 200 {
		t.Fatalf("unexpected events offsets: %+v", analyticsOffsets["events"])
	}
	if analyticsOffsets["logs"][0] != 50 {
		t.Fatalf("unexpected logs offset: %d", analyticsOffsets["logs"][0])
	}

	// Verify group listing
	groups := om.ListGroups()
	if len(groups) != 2 {
		t.Fatalf("expected 2 distinct groups, got %d", len(groups))
	}
}

func TestConsumer_OffsetManagerMonotonicOverwrite(t *testing.T) {
	tempDir := t.TempDir()
	cfg := consumer.OffsetManagerConfig{
		Directory: tempDir,
	}

	om, err := consumer.OpenOffsetManager(cfg)
	if err != nil {
		t.Fatalf("failed to open offset manager: %v", err)
	}
	defer om.Close()

	// Initial commit
	if err := om.CommitOffset(consumer.Checkpoint{GroupID: "g1", Topic: "t1", PartitionID: 0, Offset: 10}); err != nil {
		t.Fatalf("commit 1 failed: %v", err)
	}

	// Overwrite commit
	if err := om.CommitOffset(consumer.Checkpoint{GroupID: "g1", Topic: "t1", PartitionID: 0, Offset: 25}); err != nil {
		t.Fatalf("commit 2 failed: %v", err)
	}

	offset, err := om.GetOffset("g1", "t1", 0)
	if err != nil {
		t.Fatalf("get offset failed: %v", err)
	}
	if offset != 25 {
		t.Fatalf("expected overwritten offset 25, got %d", offset)
	}
}

func TestConsumer_OffsetManagerCrashRestartRecovery(t *testing.T) {
	tempDir := t.TempDir()
	cfg := consumer.OffsetManagerConfig{
		Directory:    tempDir,
		SyncOnCommit: true,
	}

	om, err := consumer.OpenOffsetManager(cfg)
	if err != nil {
		t.Fatalf("failed to open offset manager: %v", err)
	}

	// Commit checkpoints across multiple groups
	for p := uint32(0); p < 5; p++ {
		cp := consumer.Checkpoint{
			GroupID:     "crash-group",
			Topic:       "telemetry",
			PartitionID: p,
			Offset:      uint64(1000 + p*50),
			Metadata:    fmt.Sprintf("meta-p%d", p),
		}
		if err := om.CommitOffset(cp); err != nil {
			t.Fatalf("commit partition %d failed: %v", p, err)
		}
	}

	// Clean close
	if err := om.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Reopen on existing directory
	recoveredOM, err := consumer.OpenOffsetManager(cfg)
	if err != nil {
		t.Fatalf("failed to recover offset manager: %v", err)
	}
	defer recoveredOM.Close()

	// Validate all checkpoints replayed accurately into cache
	for p := uint32(0); p < 5; p++ {
		cp, err := recoveredOM.GetCheckpoint("crash-group", "telemetry", p)
		if err != nil {
			t.Fatalf("failed to get recovered checkpoint for partition %d: %v", p, err)
		}
		expectedOffset := uint64(1000 + p*50)
		if cp.Offset != expectedOffset {
			t.Fatalf("expected offset %d, got %d", expectedOffset, cp.Offset)
		}
		expectedMeta := fmt.Sprintf("meta-p%d", p)
		if cp.Metadata != expectedMeta {
			t.Fatalf("expected meta %s, got %s", expectedMeta, cp.Metadata)
		}
	}
}

func TestConsumer_OffsetManagerConcurrentCommitsAndReads(t *testing.T) {
	tempDir := t.TempDir()
	cfg := consumer.OffsetManagerConfig{
		Directory: tempDir,
	}

	om, err := consumer.OpenOffsetManager(cfg)
	if err != nil {
		t.Fatalf("failed to open offset manager: %v", err)
	}
	defer om.Close()

	const numWorkers = 8
	const commitsPerWorker = 50
	var wg sync.WaitGroup
	var commitErrors uint64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < commitsPerWorker; i++ {
				cp := consumer.Checkpoint{
					GroupID:     fmt.Sprintf("worker-group-%d", workerID),
					Topic:       "stream",
					PartitionID: uint32(i % 4),
					Offset:      uint64(i + 1),
				}
				if err := om.CommitOffset(cp); err != nil {
					atomic.AddUint64(&commitErrors, 1)
				}

				// Concurrent read
				_, _ = om.GetOffset(cp.GroupID, cp.Topic, cp.PartitionID)
				time.Sleep(100 * time.Microsecond)
			}
		}(w)
	}

	wg.Wait()

	if commitErrors > 0 {
		t.Fatalf("encountered %d errors during concurrent commits", commitErrors)
	}

	groups := om.ListGroups()
	if len(groups) != numWorkers {
		t.Fatalf("expected %d groups, got %d", numWorkers, len(groups))
	}
}

func TestConsumer_OffsetManagerClosedOperations(t *testing.T) {
	tempDir := t.TempDir()
	om, err := consumer.OpenOffsetManager(consumer.OffsetManagerConfig{Directory: tempDir})
	if err != nil {
		t.Fatalf("failed to open offset manager: %v", err)
	}

	if err := om.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Repeated close is idempotent
	if err := om.Close(); err != nil {
		t.Fatalf("idempotent close failed: %v", err)
	}

	// Commit on closed
	err = om.CommitOffset(consumer.Checkpoint{GroupID: "g", Topic: "t", PartitionID: 0, Offset: 1})
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected os.ErrClosed on commit, got %v", err)
	}

	// Get on closed
	_, err = om.GetOffset("g", "t", 0)
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected os.ErrClosed on get, got %v", err)
	}

	// Sync on closed
	err = om.Sync()
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected os.ErrClosed on sync, got %v", err)
	}
}

func TestConsumer_OffsetManagerRecoveryAfterTruncation(t *testing.T) {
	tempDir := t.TempDir()
	cfg := consumer.OffsetManagerConfig{
		Directory:       tempDir,
		MaxSegmentBytes: 200, // Small segment to force multiple segments
		SyncOnCommit:    true,
	}

	om, err := consumer.OpenOffsetManager(cfg)
	if err != nil {
		t.Fatalf("failed open offset manager: %v", err)
	}

	// Commit 15 checkpoints
	for i := uint32(0); i < 15; i++ {
		cp := consumer.Checkpoint{
			GroupID:     "pruned-grp",
			Topic:       "events",
			PartitionID: i,
			Offset:      uint64(500 + i),
		}
		if err := om.CommitOffset(cp); err != nil {
			t.Fatalf("commit %d failed: %v", i, err)
		}
	}

	// Truncate early offset segments
	pruned, err := om.TruncateBefore(5)
	if err != nil {
		t.Fatalf("truncate failed: %v", err)
	}
	if pruned == 0 {
		t.Fatalf("expected at least 1 pruned segment")
	}

	if err := om.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Reopen offset manager: replayLog must start from OldestOffset and not abort at 0
	reopened, err := consumer.OpenOffsetManager(cfg)
	if err != nil {
		t.Fatalf("reopen after truncation failed: %v", err)
	}
	defer reopened.Close()

	// Verify the latest surviving checkpoint is present in cache
	latestOffset, err := reopened.GetOffset("pruned-grp", "events", 14)
	if err != nil {
		t.Fatalf("failed to get checkpoint 14: %v", err)
	}
	if latestOffset != 514 {
		t.Fatalf("expected offset 514, got %d", latestOffset)
	}
}

func TestConsumer_OffsetManagerStringLengthGuards(t *testing.T) {
	tempDir := t.TempDir()
	cfg := consumer.OffsetManagerConfig{Directory: tempDir}

	om, err := consumer.OpenOffsetManager(cfg)
	if err != nil {
		t.Fatalf("failed open offset manager: %v", err)
	}
	defer om.Close()

	oversized := string(bytes.Repeat([]byte("a"), 65536))

	// Oversized group
	err = om.CommitOffset(consumer.Checkpoint{GroupID: oversized, Topic: "t", PartitionID: 0})
	if !errors.Is(err, consumer.ErrInvalidCheckpoint) {
		t.Fatalf("expected ErrInvalidCheckpoint for oversized group, got %v", err)
	}

	// Oversized topic
	err = om.CommitOffset(consumer.Checkpoint{GroupID: "g", Topic: oversized, PartitionID: 0})
	if !errors.Is(err, consumer.ErrInvalidCheckpoint) {
		t.Fatalf("expected ErrInvalidCheckpoint for oversized topic, got %v", err)
	}

	// Oversized metadata
	err = om.CommitOffset(consumer.Checkpoint{GroupID: "g", Topic: "t", PartitionID: 0, Metadata: oversized})
	if !errors.Is(err, consumer.ErrInvalidCheckpoint) {
		t.Fatalf("expected ErrInvalidCheckpoint for oversized metadata, got %v", err)
	}
}

func TestConsumer_OffsetManagerClosedQueries(t *testing.T) {
	tempDir := t.TempDir()
	om, err := consumer.OpenOffsetManager(consumer.OffsetManagerConfig{Directory: tempDir})
	if err != nil {
		t.Fatalf("failed open: %v", err)
	}

	if err := om.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	if groups := om.ListGroups(); groups != nil {
		t.Fatalf("expected nil from ListGroups on closed manager, got %v", groups)
	}
	if offsets := om.GetGroupOffsets("grp"); offsets != nil {
		t.Fatalf("expected nil from GetGroupOffsets on closed manager, got %v", offsets)
	}
	if _, err := om.CleanSegments(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected os.ErrClosed on CleanSegments, got %v", err)
	}
	if _, err := om.TruncateBefore(1); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected os.ErrClosed on TruncateBefore, got %v", err)
	}
}
