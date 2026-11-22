// Package: consumer
// File: offset_manager.go
// Core Work: Consumer offset checkpoint manager, WAL partition bootstrap, and log replay
// Exported: ErrOffsetNotFound, ErrInvalidCheckpoint, CheckpointKey, Checkpoint, OffsetManagerConfig, OffsetManager, OpenOffsetManager
package consumer

/*
Functions Manifest & Usage:
- (c Checkpoint) Key() CheckpointKey: Derives lookup key from Checkpoint.
- OpenOffsetManager(cfg OffsetManagerConfig) (*OffsetManager, error): Opens/creates commit stream partition and replays state.
- (om *OffsetManager) replayLog() error: Scans checkpoint log from oldest offset and populates in-memory cache.
- (om *OffsetManager) CleanSegments() (int, error): Prunes old checkpoint segments according to retention policy.
- (om *OffsetManager) TruncateBefore(targetOffset uint64) (int, error): Truncates checkpoint partition segments before targetOffset.
- (om *OffsetManager) Sync() error: Flushes checkpoint log partition to physical storage.
- (om *OffsetManager) Close() error: Closes underlying partition and seals cache.
*/

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/barbaravane/walite/internal/log"
)

const (
	checkpointMagic   uint16 = 0x574F // "WO" for Walite Offset
	checkpointVersion uint8  = 1
)

var (
	// ErrOffsetNotFound is returned when no committed checkpoint exists for the requested target.
	ErrOffsetNotFound = errors.New("walite: consumer group offset not found")

	// ErrInvalidCheckpoint indicates malformed checkpoint binary payload or key.
	ErrInvalidCheckpoint = errors.New("walite: invalid checkpoint format")
)

// CheckpointKey uniquely identifies a tracked offset target.
type CheckpointKey struct {
	GroupID     string
	Topic       string
	PartitionID uint32
}

// Checkpoint represents a committed offset milestone with timestamp and user metadata.
type Checkpoint struct {
	GroupID     string
	Topic       string
	PartitionID uint32
	Offset      uint64
	Timestamp   int64
	Metadata    string
}

// Key returns the CheckpointKey associated with this checkpoint.
func (c Checkpoint) Key() CheckpointKey {
	return CheckpointKey{
		GroupID:     c.GroupID,
		Topic:       c.Topic,
		PartitionID: c.PartitionID,
	}
}

// OffsetManagerConfig specifies persistence directory and durability controls for checkpoints.
type OffsetManagerConfig struct {
	Directory       string
	SyncOnCommit    bool
	MaxSegmentBytes uint64
	Cleaner         log.CleanerConfig
}

// OffsetManager provides crash-safe, fast in-memory queryable consumer offset tracking.
type OffsetManager struct {
	mu        sync.RWMutex
	dir       string
	cfg       OffsetManagerConfig
	partition *log.Partition
	cache     map[CheckpointKey]Checkpoint
	closed    bool
}

// OpenOffsetManager initializes or recovers an offset manager at cfg.Directory.
// It replays existing commit records from the underlying partition into an in-memory cache.
func OpenOffsetManager(cfg OffsetManagerConfig) (*OffsetManager, error) {
	if cfg.Directory == "" {
		return nil, errors.New("walite: offset manager directory path is required")
	}

	if cfg.MaxSegmentBytes == 0 {
		cfg.MaxSegmentBytes = 10 * 1024 * 1024 // 10MB default segment limit
	}

	offsetsDir := filepath.Join(cfg.Directory, "offsets")
	pCfg := log.PartitionConfig{
		Directory:          offsetsDir,
		MaxSegmentBytes:    cfg.MaxSegmentBytes,
		IndexIntervalBytes: 1024,
		Cleaner:            cfg.Cleaner,
	}

	if cfg.SyncOnCommit {
		pCfg.Flush.Policy = log.SyncAlways
	}

	p, err := log.OpenPartition(pCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open offset partition: %w", err)
	}

	om := &OffsetManager{
		dir:       cfg.Directory,
		cfg:       cfg,
		partition: p,
		cache:     make(map[CheckpointKey]Checkpoint),
	}

	if err := om.replayLog(); err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("failed to replay offset checkpoints: %w", err)
	}

	return om, nil
}

func (om *OffsetManager) replayLog() error {
	nextOffset := om.partition.NextOffset()
	for off := om.partition.OldestOffset(); off < nextOffset; off++ {
		rec, err := om.partition.ReadAt(off)
		if err != nil {
			if errors.Is(err, log.ErrOffsetOutOfRange) {
				// If an offset was pruned between segments, advance to current oldest offset
				curOldest := om.partition.OldestOffset()
				if off < curOldest {
					off = curOldest - 1
					continue
				}
				// Reached end of partition
				break
			}
			return fmt.Errorf("failed to read checkpoint at log offset %d: %w", off, err)
		}

		key, err := decodeCheckpointKey(rec.Key)
		if err != nil {
			// Skip corrupt or unparseable keys during recovery
			continue
		}

		offset, timestamp, metadata, err := decodeCheckpointValue(rec.Value)
		if err != nil {
			continue
		}

		om.cache[key] = Checkpoint{
			GroupID:     key.GroupID,
			Topic:       key.Topic,
			PartitionID: key.PartitionID,
			Offset:      offset,
			Timestamp:   timestamp,
			Metadata:    metadata,
		}
	}
	return nil
}

// CommitOffset records a consumer group checkpoint to persistent storage and updates the in-memory cache.

func (om *OffsetManager) CleanSegments() (int, error) {
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.closed {
		return 0, os.ErrClosed
	}
	return om.partition.CleanSegments()
}

// TruncateBefore permanently removes sealed historical offset segments before targetOffset.
func (om *OffsetManager) TruncateBefore(targetOffset uint64) (int, error) {
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.closed {
		return 0, os.ErrClosed
	}
	return om.partition.TruncateBefore(targetOffset)
}

// Sync flushes all pending checkpoint writes to physical storage.
func (om *OffsetManager) Sync() error {
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.closed {
		return os.ErrClosed
	}
	return om.partition.Sync()
}

// Close flushes and closes the underlying checkpoint partition.
func (om *OffsetManager) Close() error {
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.closed {
		return nil
	}
	om.closed = true
	return om.partition.Close()
}

// Binary serialization helpers

