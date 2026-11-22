// Package: consumer
// File: offset_manager_commit.go
// Core Work: Consumer offset commit serialization and in-memory watermark cache queries
// Exported: none (methods on OffsetManager: CommitOffset, CommitOffsets, GetOffset, GetCheckpoint, GetGroupOffsets, ListGroups)
package consumer

/*
Functions Manifest & Usage:
- (om *OffsetManager) CommitOffset(cp Checkpoint) error: Appends single offset checkpoint to log and updates cache.
- (om *OffsetManager) CommitOffsets(checkpoints []Checkpoint) error: Atomically appends batch of checkpoints to log and cache.
- (om *OffsetManager) GetOffset(groupID, topic string, partitionID uint32) (uint64, error): Fast in-memory lookup of committed offset.
- (om *OffsetManager) GetCheckpoint(groupID, topic string, partitionID uint32) (Checkpoint, error): Retrieves full Checkpoint structure including metadata.
- (om *OffsetManager) GetGroupOffsets(groupID string) map[string]map[uint32]uint64: Queries all committed offsets for a given consumer group.
- (om *OffsetManager) ListGroups() []string: Returns sorted list of all active consumer group IDs.
*/

import (
	"fmt"
	"os"
	"time"

	"github.com/barbaravane/walite/internal/log"
)

func (om *OffsetManager) CommitOffset(cp Checkpoint) error {
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.closed {
		return os.ErrClosed
	}

	if len(cp.GroupID) > 65535 || len(cp.Topic) > 65535 || len(cp.Metadata) > 65535 {
		return ErrInvalidCheckpoint
	}

	if cp.Timestamp == 0 {
		cp.Timestamp = time.Now().UnixNano()
	}

	keyBuf := encodeCheckpointKey(cp.Key())
	valBuf := encodeCheckpointValue(cp.Offset, cp.Timestamp, cp.Metadata)

	rec := &log.Record{
		Key:   keyBuf,
		Value: valBuf,
	}

	if _, err := om.partition.Append(rec); err != nil {
		return fmt.Errorf("failed to append checkpoint record: %w", err)
	}

	om.cache[cp.Key()] = cp
	return nil
}

// CommitOffsets commits a batch of checkpoints atomically into the persistent commit stream.
func (om *OffsetManager) CommitOffsets(checkpoints []Checkpoint) error {
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.closed {
		return os.ErrClosed
	}

	now := time.Now().UnixNano()
	for _, cp := range checkpoints {
		if len(cp.GroupID) > 65535 || len(cp.Topic) > 65535 || len(cp.Metadata) > 65535 {
			return ErrInvalidCheckpoint
		}

		if cp.Timestamp == 0 {
			cp.Timestamp = now
		}

		keyBuf := encodeCheckpointKey(cp.Key())
		valBuf := encodeCheckpointValue(cp.Offset, cp.Timestamp, cp.Metadata)

		rec := &log.Record{
			Key:   keyBuf,
			Value: valBuf,
		}

		if _, err := om.partition.Append(rec); err != nil {
			return fmt.Errorf("failed to append batch checkpoint for %s/%s/%d: %w",
				cp.GroupID, cp.Topic, cp.PartitionID, err)
		}

		om.cache[cp.Key()] = cp
	}

	return nil
}

// GetOffset returns the committed offset for the specified group, topic, and partition.
func (om *OffsetManager) GetOffset(groupID, topic string, partitionID uint32) (uint64, error) {
	om.mu.RLock()
	defer om.mu.RUnlock()

	if om.closed {
		return 0, os.ErrClosed
	}

	key := CheckpointKey{
		GroupID:     groupID,
		Topic:       topic,
		PartitionID: partitionID,
	}

	cp, exists := om.cache[key]
	if !exists {
		return 0, ErrOffsetNotFound
	}

	return cp.Offset, nil
}

// GetCheckpoint returns the complete committed checkpoint for the specified target.
func (om *OffsetManager) GetCheckpoint(groupID, topic string, partitionID uint32) (Checkpoint, error) {
	om.mu.RLock()
	defer om.mu.RUnlock()

	if om.closed {
		return Checkpoint{}, os.ErrClosed
	}

	key := CheckpointKey{
		GroupID:     groupID,
		Topic:       topic,
		PartitionID: partitionID,
	}

	cp, exists := om.cache[key]
	if !exists {
		return Checkpoint{}, ErrOffsetNotFound
	}

	return cp, nil
}

// GetGroupOffsets returns all committed offsets for a given consumer group organized by topic and partition.
func (om *OffsetManager) GetGroupOffsets(groupID string) map[string]map[uint32]uint64 {
	om.mu.RLock()
	defer om.mu.RUnlock()

	if om.closed {
		return nil
	}

	result := make(map[string]map[uint32]uint64)
	for key, cp := range om.cache {
		if key.GroupID == groupID {
			if _, exists := result[key.Topic]; !exists {
				result[key.Topic] = make(map[uint32]uint64)
			}
			result[key.Topic][key.PartitionID] = cp.Offset
		}
	}

	return result
}

// ListGroups returns a slice of all consumer group IDs currently registered in the manager.
func (om *OffsetManager) ListGroups() []string {
	om.mu.RLock()
	defer om.mu.RUnlock()

	if om.closed {
		return nil
	}

	groupMap := make(map[string]struct{})
	for key := range om.cache {
		groupMap[key.GroupID] = struct{}{}
	}

	groups := make([]string, 0, len(groupMap))
	for g := range groupMap {
		groups = append(groups, g)
	}
	return groups
}

// CleanSegments applies configured retention policies to purge eligible historical offset segments.
