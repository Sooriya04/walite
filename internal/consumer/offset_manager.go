// Package: consumer
// File: offset_manager.go
// Core Work: Consumer group offset checkpointing, crash recovery journal, and watermark query cache
// Exported: OffsetManagerConfig, OffsetManager, Checkpoint, CheckpointKey, ErrOffsetNotFound, ErrInvalidCheckpoint, OpenOffsetManager
package consumer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

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

func encodeCheckpointKey(key CheckpointKey) []byte {
	groupBytes := []byte(key.GroupID)
	topicBytes := []byte(key.Topic)

	totalLen := 2 + len(groupBytes) + 2 + len(topicBytes) + 4
	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint16(buf[0:2], uint16(len(groupBytes)))
	copy(buf[2:2+len(groupBytes)], groupBytes)

	pos := 2 + len(groupBytes)
	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(topicBytes)))
	pos += 2
	copy(buf[pos:pos+len(topicBytes)], topicBytes)
	pos += len(topicBytes)

	binary.BigEndian.PutUint32(buf[pos:pos+4], key.PartitionID)
	return buf
}

func decodeCheckpointKey(buf []byte) (CheckpointKey, error) {
	if len(buf) < 8 {
		return CheckpointKey{}, ErrInvalidCheckpoint
	}

	groupLen := int(binary.BigEndian.Uint16(buf[0:2]))
	if len(buf) < 2+groupLen+2+4 {
		return CheckpointKey{}, ErrInvalidCheckpoint
	}
	groupID := string(buf[2 : 2+groupLen])

	pos := 2 + groupLen
	topicLen := int(binary.BigEndian.Uint16(buf[pos : pos+2]))
	pos += 2
	if len(buf) < pos+topicLen+4 {
		return CheckpointKey{}, ErrInvalidCheckpoint
	}
	topic := string(buf[pos : pos+topicLen])
	pos += topicLen

	partitionID := binary.BigEndian.Uint32(buf[pos : pos+4])

	return CheckpointKey{
		GroupID:     groupID,
		Topic:       topic,
		PartitionID: partitionID,
	}, nil
}

func encodeCheckpointValue(offset uint64, timestamp int64, metadata string) []byte {
	metaBytes := []byte(metadata)
	totalLen := 2 + 1 + 8 + 8 + 2 + len(metaBytes)
	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint16(buf[0:2], checkpointMagic)
	buf[2] = checkpointVersion
	binary.BigEndian.PutUint64(buf[3:11], offset)
	binary.BigEndian.PutUint64(buf[11:19], uint64(timestamp))
	binary.BigEndian.PutUint16(buf[19:21], uint16(len(metaBytes)))
	copy(buf[21:], metaBytes)

	return buf
}

func decodeCheckpointValue(buf []byte) (uint64, int64, string, error) {
	if len(buf) < 21 {
		return 0, 0, "", ErrInvalidCheckpoint
	}

	magic := binary.BigEndian.Uint16(buf[0:2])
	if magic != checkpointMagic {
		return 0, 0, "", ErrInvalidCheckpoint
	}

	version := buf[2]
	if version != checkpointVersion {
		return 0, 0, "", ErrInvalidCheckpoint
	}

	offset := binary.BigEndian.Uint64(buf[3:11])
	timestamp := int64(binary.BigEndian.Uint64(buf[11:19]))
	metaLen := int(binary.BigEndian.Uint16(buf[19:21]))

	if len(buf) < 21+metaLen {
		return 0, 0, "", ErrInvalidCheckpoint
	}

	metadata := string(buf[21 : 21+metaLen])
	return offset, timestamp, metadata, nil
}
