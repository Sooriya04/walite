// Package: test
// File: consumer_rebalance_test.go
// Core Work: Consumer group partition assignment strategies, heartbeat timeouts, and duplicate prevention tests
// Exported: none (test package: TestConsumer_Rebalance*)
package test

/*
Functions Manifest & Usage:
- TestConsumer_RebalanceRangeStrategy: Verifies contiguous partition assignment per topic.
- TestConsumer_RebalanceRoundRobinStrategy: Tests interleaved round-robin partition distribution.
- TestConsumer_RebalanceMemberLeaveAndTimeout: Validates member eviction on timeout and explicit leave.
- TestConsumer_DuplicatePreventionAndResumeExactOffset: Tests crash resume offset continuity and zero duplicate processing.
- TestConsumer_ActiveMembersTimeoutFiltering: Ensures ActiveMembers() excludes expired heartbeats.
*/

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/barbaravane/walite/internal/consumer"
	"github.com/barbaravane/walite/internal/log"
)

func TestConsumer_RebalanceRangeStrategy(t *testing.T) {
	cfg := consumer.GroupCoordinatorConfig{
		GroupID:  "range-group",
		Strategy: consumer.StrategyRange,
	}
	gc := consumer.NewGroupCoordinator(cfg)

	// Register 8 partitions for topic "events"
	gc.RegisterPartitions("events", []uint32{0, 1, 2, 3, 4, 5, 6, 7})

	// 3 consumer members join
	_, _ = gc.JoinGroup("member-1", []string{"events"})
	_, _ = gc.JoinGroup("member-2", []string{"events"})
	_, _ = gc.JoinGroup("member-3", []string{"events"})

	assignments, gen, err := gc.Rebalance()
	if err != nil {
		t.Fatalf("rebalance failed: %v", err)
	}
	if gen != 1 {
		t.Fatalf("expected generation 1, got %d", gen)
	}

	// 8 partitions / 3 members: member-1 gets 3, member-2 gets 3, member-3 gets 2
	if len(assignments["member-1"]) != 3 {
		t.Fatalf("expected 3 partitions for member-1, got %d", len(assignments["member-1"]))
	}
	if len(assignments["member-2"]) != 3 {
		t.Fatalf("expected 3 partitions for member-2, got %d", len(assignments["member-2"]))
	}
	if len(assignments["member-3"]) != 2 {
		t.Fatalf("expected 2 partitions for member-3, got %d", len(assignments["member-3"]))
	}

	// Verify contiguous ranges
	if assignments["member-1"][0].PartitionID != 0 || assignments["member-1"][2].PartitionID != 2 {
		t.Fatalf("unexpected range for member-1: %+v", assignments["member-1"])
	}
	if assignments["member-2"][0].PartitionID != 3 || assignments["member-2"][2].PartitionID != 5 {
		t.Fatalf("unexpected range for member-2: %+v", assignments["member-2"])
	}
	if assignments["member-3"][0].PartitionID != 6 || assignments["member-3"][1].PartitionID != 7 {
		t.Fatalf("unexpected range for member-3: %+v", assignments["member-3"])
	}
}

func TestConsumer_RebalanceRoundRobinStrategy(t *testing.T) {
	cfg := consumer.GroupCoordinatorConfig{
		GroupID:  "rr-group",
		Strategy: consumer.StrategyRoundRobin,
	}
	gc := consumer.NewGroupCoordinator(cfg)

	// 2 topics with 3 partitions each = 6 total targets
	gc.RegisterPartitions("topic-A", []uint32{0, 1, 2})
	gc.RegisterPartitions("topic-B", []uint32{0, 1, 2})

	_, _ = gc.JoinGroup("m1", []string{"topic-A", "topic-B"})
	_, _ = gc.JoinGroup("m2", []string{"topic-A", "topic-B"})
	_, _ = gc.JoinGroup("m3", []string{"topic-A", "topic-B"})

	assignments, gen, err := gc.Rebalance()
	if err != nil {
		t.Fatalf("rebalance failed: %v", err)
	}
	if gen != 1 {
		t.Fatalf("expected generation 1, got %d", gen)
	}

	// Each member should have exactly 2 partitions
	for _, id := range []string{"m1", "m2", "m3"} {
		if len(assignments[id]) != 2 {
			t.Fatalf("expected 2 partitions for %s, got %d", id, len(assignments[id]))
		}
	}
}

func TestConsumer_RebalanceMemberLeaveAndTimeout(t *testing.T) {
	cfg := consumer.GroupCoordinatorConfig{
		GroupID:        "dyn-group",
		Strategy:       consumer.StrategyRange,
		SessionTimeout: 25 * time.Millisecond,
	}
	gc := consumer.NewGroupCoordinator(cfg)
	gc.RegisterPartitions("data", []uint32{0, 1, 2, 3})

	_, _ = gc.JoinGroup("consumer-A", []string{"data"})
	_, _ = gc.JoinGroup("consumer-B", []string{"data"})

	// Rebalance with 2 consumers
	assignments, _, err := gc.Rebalance()
	if err != nil {
		t.Fatalf("rebalance 1 failed: %v", err)
	}
	if len(assignments["consumer-A"]) != 2 || len(assignments["consumer-B"]) != 2 {
		t.Fatalf("expected 2 partitions each, got %+v", assignments)
	}

	// Consumer-B explicitly leaves
	if err := gc.LeaveGroup("consumer-B"); err != nil {
		t.Fatalf("leave group failed: %v", err)
	}

	assignmentsAfterLeave, genAfterLeave, err := gc.Rebalance()
	if err != nil {
		t.Fatalf("rebalance after leave failed: %v", err)
	}
	if genAfterLeave != 2 {
		t.Fatalf("expected generation 2, got %d", genAfterLeave)
	}
	// All 4 partitions assigned to consumer-A
	if len(assignmentsAfterLeave["consumer-A"]) != 4 {
		t.Fatalf("expected all 4 partitions for consumer-A, got %d", len(assignmentsAfterLeave["consumer-A"]))
	}

	// Test SessionTimeout eviction
	time.Sleep(35 * time.Millisecond) // consumer-A expires without heartbeat
	_, _, err = gc.Rebalance()
	if !errors.Is(err, consumer.ErrGroupEmpty) {
		t.Fatalf("expected ErrGroupEmpty after timeout eviction, got %v", err)
	}
}

func TestConsumer_DuplicatePreventionAndResumeExactOffset(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Storage partition
	partitionDir := tempDir + "/partition"
	p, err := log.OpenPartition(log.PartitionConfig{
		Directory:          partitionDir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 128,
	})
	if err != nil {
		t.Fatalf("failed to open partition: %v", err)
	}
	defer p.Close()

	// 2. Offset Manager
	offsetDir := tempDir + "/offsets"
	om, err := consumer.OpenOffsetManager(consumer.OffsetManagerConfig{
		Directory: offsetDir,
	})
	if err != nil {
		t.Fatalf("failed to open offset manager: %v", err)
	}
	defer om.Close()

	// Producer writes 20 records (offsets 0..19)
	for i := 0; i < 20; i++ {
		rec := &log.Record{
			Key:   []byte(fmt.Sprintf("k-%d", i)),
			Value: []byte(fmt.Sprintf("v-%d", i)),
		}
		offset, err := p.Append(rec)
		if err != nil {
			t.Fatalf("produce %d failed: %v", i, err)
		}
		if offset != uint64(i) {
			t.Fatalf("expected offset %d, got %d", i, offset)
		}
	}

	// Consumer 1 reads first 10 records (0..9)
	var consumer1Processed []uint64
	for off := uint64(0); off < 10; off++ {
		rec, err := p.ReadAt(off)
		if err != nil {
			t.Fatalf("consumer 1 read %d failed: %v", off, err)
		}
		if rec.Offset != off {
			t.Fatalf("expected record offset %d, got %d", off, rec.Offset)
		}
		consumer1Processed = append(consumer1Processed, off)
	}

	// Consumer 1 checkpoints next starting offset = 10
	err = om.CommitOffset(consumer.Checkpoint{
		GroupID:     "my-group",
		Topic:       "telemetry",
		PartitionID: 0,
		Offset:      10,
		Metadata:    "processed-first-10",
	})
	if err != nil {
		t.Fatalf("commit checkpoint failed: %v", err)
	}

	// Consumer 1 crashes / restarts. Consumer 2 boots up and queries starting offset:
	startOffset, err := om.GetOffset("my-group", "telemetry", 0)
	if err != nil {
		t.Fatalf("get committed offset failed: %v", err)
	}
	if startOffset != 10 {
		t.Fatalf("expected resume offset 10, got %d", startOffset)
	}

	// Consumer 2 resumes strictly from offset 10 to end (19)
	var consumer2Processed []uint64
	for off := startOffset; off < p.NextOffset(); off++ {
		rec, err := p.ReadAt(off)
		if err != nil {
			t.Fatalf("consumer 2 read %d failed: %v", off, err)
		}
		if rec.Offset != off {
			t.Fatalf("expected consumer 2 record offset %d, got %d", off, rec.Offset)
		}
		consumer2Processed = append(consumer2Processed, off)
	}

	// Verify total records processed = 20, zero duplicates, zero gaps
	allProcessed := append(consumer1Processed, consumer2Processed...)
	if len(allProcessed) != 20 {
		t.Fatalf("expected 20 processed records, got %d", len(allProcessed))
	}
	for i := 0; i < 20; i++ {
		if allProcessed[i] != uint64(i) {
			t.Fatalf("mismatched or duplicate offset at index %d: got %d", i, allProcessed[i])
		}
	}
}

func TestConsumer_ActiveMembersTimeoutFiltering(t *testing.T) {
	gc := consumer.NewGroupCoordinator(consumer.GroupCoordinatorConfig{
		GroupID:        "timeout-filter-group",
		SessionTimeout: 20 * time.Millisecond,
	})

	_, _ = gc.JoinGroup("member-active", []string{"topic-1"})
	_, _ = gc.JoinGroup("member-dead", []string{"topic-1"})

	time.Sleep(30 * time.Millisecond)

	// Refresh heartbeat only for member-active
	if err := gc.Heartbeat("member-active"); err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	active := gc.ActiveMembers()
	if len(active) != 1 || active[0] != "member-active" {
		t.Fatalf("expected only member-active in ActiveMembers(), got %v", active)
	}
}

