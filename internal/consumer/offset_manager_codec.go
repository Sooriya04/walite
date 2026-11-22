// Package: consumer
// File: offset_manager_codec.go
// Core Work: Binary framing, serialization, and deserialization of consumer checkpoint keys and values
// Exported: none (internal functions: encodeCheckpointKey, decodeCheckpointKey, encodeCheckpointValue, decodeCheckpointValue)
package consumer

/*
Functions Manifest & Usage:
- encodeCheckpointKey(key CheckpointKey) []byte: Binary encodes groupID, topic, partitionID into log record key.
- decodeCheckpointKey(buf []byte) (CheckpointKey, error): Deserializes binary buffer into CheckpointKey.
- encodeCheckpointValue(offset uint64, timestamp int64, metadata string) []byte: Binary encodes offset, timestamp, and metadata.
- decodeCheckpointValue(buf []byte) (uint64, int64, string, error): Deserializes binary value buffer into checkpoint fields.
*/

import (
	"encoding/binary"
)

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
