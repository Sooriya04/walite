// Package: protocol
// File: messages.go
// Core Work: Strongly typed protocol request/response structures and high-speed binary payload serialization.
// Exported: ProduceReq, ProduceResp, ConsumeReq, ConsumeResp, OffsetReq, OffsetResp, CommitReq, CommitResp, PingReq, PongResp, EncodeProduceReq, DecodeProduceReq, EncodeProduceResp, DecodeProduceResp, EncodeConsumeReq, DecodeConsumeReq, EncodeConsumeResp, DecodeConsumeResp, EncodeOffsetReq, DecodeOffsetReq, EncodeOffsetResp, DecodeOffsetResp, EncodeCommitReq, DecodeCommitReq, EncodeCommitResp, DecodeCommitResp, EncodePingReq, DecodePingReq, EncodePongResp, DecodePongResp
package protocol

/*
Functions Manifest & Usage:
- EncodeProduceReq(req ProduceReq) []byte: Binary encodes a produce request payload.
- DecodeProduceReq(data []byte) (ProduceReq, error): Decodes binary payload into a ProduceReq.
- EncodeProduceResp(resp ProduceResp) []byte: Binary encodes a produce response payload.
- DecodeProduceResp(data []byte) (ProduceResp, error): Decodes binary payload into a ProduceResp.
- EncodeConsumeReq(req ConsumeReq) []byte: Binary encodes a consume request payload.
- DecodeConsumeReq(data []byte) (ConsumeReq, error): Decodes binary payload into a ConsumeReq.
- EncodeConsumeResp(resp ConsumeResp) []byte: Binary encodes a consume response payload.
- DecodeConsumeResp(data []byte) (ConsumeResp, error): Decodes binary payload into a ConsumeResp.
- EncodeOffsetReq(req OffsetReq) []byte: Binary encodes an offset query request payload.
- DecodeOffsetReq(data []byte) (OffsetReq, error): Decodes binary payload into an OffsetReq.
- EncodeOffsetResp(resp OffsetResp) []byte: Binary encodes an offset query response payload.
- DecodeOffsetResp(data []byte) (OffsetResp, error): Decodes binary payload into an OffsetResp.
- EncodeCommitReq(req CommitReq) []byte: Binary encodes an offset commit request payload.
- DecodeCommitReq(data []byte) (CommitReq, error): Decodes binary payload into a CommitReq.
- EncodeCommitResp(resp CommitResp) []byte: Binary encodes an offset commit response payload.
- DecodeCommitResp(data []byte) (CommitResp, error): Decodes binary payload into a CommitResp.
- EncodePingReq(req PingReq) []byte: Encodes a ping heartbeat payload.
- DecodePingReq(data []byte) (PingReq, error): Decodes a ping heartbeat payload.
- EncodePongResp(resp PongResp) []byte: Encodes a pong heartbeat response payload.
- DecodePongResp(data []byte) (PongResp, error): Decodes a pong heartbeat response payload.
*/

import (
	"encoding/binary"

	"github.com/barbaravane/walite/internal/log"
)

// ProduceReq carries record batch publish data.
type ProduceReq struct {
	Topic       string
	PartitionID uint32
	Records     []*log.Record
}

// ProduceResp acknowledges produce completion.
type ProduceResp struct {
	Status     uint16
	BaseOffset uint64
	Count      uint32
}

// ConsumeReq requests sequential records from a topic partition.
type ConsumeReq struct {
	Topic       string
	PartitionID uint32
	Offset      uint64
	MaxBytes    uint32
}

// ConsumeResp returns fetched records.
type ConsumeResp struct {
	Status  uint16
	Records []*log.Record
}

// OffsetReq queries a consumer group's committed offset.
type OffsetReq struct {
	GroupID     string
	Topic       string
	PartitionID uint32
}

// OffsetResp returns the committed offset.
type OffsetResp struct {
	Status uint16
	Offset uint64
}

// CommitReq commits a consumer group offset.
type CommitReq struct {
	GroupID     string
	Topic       string
	PartitionID uint32
	Offset      uint64
	Metadata    string
}

// CommitResp acknowledges an offset commit.
type CommitResp struct {
	Status uint16
}

// PingReq is a heartbeat ping request.
type PingReq struct {
	Echo []byte
}

// PongResp is a heartbeat pong response.
type PongResp struct {
	Echo []byte
}

// maxStringBytes defines the 16-bit ceiling (65,535 bytes) for wire-encoded string fields.
const maxStringBytes = 65535

func clampBytes(b []byte) []byte {
	if len(b) > maxStringBytes {
		return b[:maxStringBytes]
	}
	return b
}

// EncodeProduceReq serializes a ProduceReq into a binary payload.
func EncodeProduceReq(req ProduceReq) []byte {
	topicBytes := clampBytes([]byte(req.Topic))
	totalRecBytes := 0
	for _, r := range req.Records {
		totalRecBytes += log.HeaderSize + len(r.Key) + len(r.Value)
	}

	totalLen := 2 + len(topicBytes) + 4 + 4 + totalRecBytes
	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint16(buf[0:2], uint16(len(topicBytes)))
	copy(buf[2:2+len(topicBytes)], topicBytes)
	pos := 2 + len(topicBytes)

	binary.BigEndian.PutUint32(buf[pos:pos+4], req.PartitionID)
	pos += 4

	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(req.Records)))
	pos += 4

	for _, r := range req.Records {
		recBytes := log.EncodeRecord(buf[pos:], r)
		pos += len(recBytes)
	}

	return buf
}

// DecodeProduceReq deserializes binary data into a ProduceReq.
func DecodeProduceReq(data []byte) (ProduceReq, error) {
	if len(data) < 10 {
		return ProduceReq{}, ErrMalformedPayload
	}

	topicLen := int(binary.BigEndian.Uint16(data[0:2]))
	pos := 2
	if len(data) < pos+topicLen+8 {
		return ProduceReq{}, ErrMalformedPayload
	}

	topic := string(data[pos : pos+topicLen])
	pos += topicLen

	partitionID := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4

	numRecords := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	if pos+numRecords*log.HeaderSize > len(data) {
		return ProduceReq{}, ErrMalformedPayload
	}

	records := make([]*log.Record, 0, numRecords)
	for i := 0; i < numRecords; i++ {
		rec, n, err := log.DecodeRecord(data[pos:])
		if err != nil {
			return ProduceReq{}, err
		}
		records = append(records, rec)
		pos += n
	}

	return ProduceReq{
		Topic:       topic,
		PartitionID: partitionID,
		Records:     records,
	}, nil
}

// EncodeProduceResp serializes a ProduceResp into a binary payload.
func EncodeProduceResp(resp ProduceResp) []byte {
	buf := make([]byte, 14)
	binary.BigEndian.PutUint16(buf[0:2], resp.Status)
	binary.BigEndian.PutUint64(buf[2:10], resp.BaseOffset)
	binary.BigEndian.PutUint32(buf[10:14], resp.Count)
	return buf
}

// DecodeProduceResp deserializes binary data into a ProduceResp.
func DecodeProduceResp(data []byte) (ProduceResp, error) {
	if len(data) < 14 {
		return ProduceResp{}, ErrMalformedPayload
	}
	return ProduceResp{
		Status:     binary.BigEndian.Uint16(data[0:2]),
		BaseOffset: binary.BigEndian.Uint64(data[2:10]),
		Count:      binary.BigEndian.Uint32(data[10:14]),
	}, nil
}

// EncodeConsumeReq serializes a ConsumeReq into a binary payload.
func EncodeConsumeReq(req ConsumeReq) []byte {
	topicBytes := clampBytes([]byte(req.Topic))
	totalLen := 2 + len(topicBytes) + 4 + 8 + 4
	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint16(buf[0:2], uint16(len(topicBytes)))
	copy(buf[2:2+len(topicBytes)], topicBytes)
	pos := 2 + len(topicBytes)

	binary.BigEndian.PutUint32(buf[pos:pos+4], req.PartitionID)
	pos += 4

	binary.BigEndian.PutUint64(buf[pos:pos+8], req.Offset)
	pos += 8

	binary.BigEndian.PutUint32(buf[pos:pos+4], req.MaxBytes)
	return buf
}

// DecodeConsumeReq deserializes binary data into a ConsumeReq.
func DecodeConsumeReq(data []byte) (ConsumeReq, error) {
	if len(data) < 18 {
		return ConsumeReq{}, ErrMalformedPayload
	}

	topicLen := int(binary.BigEndian.Uint16(data[0:2]))
	pos := 2
	if len(data) < pos+topicLen+16 {
		return ConsumeReq{}, ErrMalformedPayload
	}

	topic := string(data[pos : pos+topicLen])
	pos += topicLen

	partitionID := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4

	offset := binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8

	maxBytes := binary.BigEndian.Uint32(data[pos : pos+4])

	return ConsumeReq{
		Topic:       topic,
		PartitionID: partitionID,
		Offset:      offset,
		MaxBytes:    maxBytes,
	}, nil
}

// EncodeConsumeResp serializes a ConsumeResp into a binary payload.
func EncodeConsumeResp(resp ConsumeResp) []byte {
	totalRecBytes := 0
	for _, r := range resp.Records {
		totalRecBytes += log.HeaderSize + len(r.Key) + len(r.Value)
	}

	totalLen := 2 + 4 + totalRecBytes
	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint16(buf[0:2], resp.Status)
	binary.BigEndian.PutUint32(buf[2:6], uint32(len(resp.Records)))
	pos := 6

	for _, r := range resp.Records {
		recBytes := log.EncodeRecord(buf[pos:], r)
		pos += len(recBytes)
	}

	return buf
}

// DecodeConsumeResp deserializes binary data into a ConsumeResp.
func DecodeConsumeResp(data []byte) (ConsumeResp, error) {
	if len(data) < 6 {
		return ConsumeResp{}, ErrMalformedPayload
	}

	status := binary.BigEndian.Uint16(data[0:2])
	numRecords := int(binary.BigEndian.Uint32(data[2:6]))
	pos := 6

	if pos+numRecords*log.HeaderSize > len(data) {
		return ConsumeResp{}, ErrMalformedPayload
	}

	records := make([]*log.Record, 0, numRecords)
	for i := 0; i < numRecords; i++ {
		rec, n, err := log.DecodeRecord(data[pos:])
		if err != nil {
			return ConsumeResp{}, err
		}
		records = append(records, rec)
		pos += n
	}

	return ConsumeResp{
		Status:  status,
		Records: records,
	}, nil
}

// EncodeOffsetReq serializes an OffsetReq into a binary payload.
func EncodeOffsetReq(req OffsetReq) []byte {
	groupBytes := clampBytes([]byte(req.GroupID))
	topicBytes := clampBytes([]byte(req.Topic))

	totalLen := 2 + len(groupBytes) + 2 + len(topicBytes) + 4
	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint16(buf[0:2], uint16(len(groupBytes)))
	copy(buf[2:2+len(groupBytes)], groupBytes)
	pos := 2 + len(groupBytes)

	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(topicBytes)))
	pos += 2
	copy(buf[pos:pos+len(topicBytes)], topicBytes)
	pos += len(topicBytes)

	binary.BigEndian.PutUint32(buf[pos:pos+4], req.PartitionID)
	return buf
}

// DecodeOffsetReq deserializes binary data into an OffsetReq.
func DecodeOffsetReq(data []byte) (OffsetReq, error) {
	if len(data) < 8 {
		return OffsetReq{}, ErrMalformedPayload
	}

	groupLen := int(binary.BigEndian.Uint16(data[0:2]))
	pos := 2
	if len(data) < pos+groupLen+6 {
		return OffsetReq{}, ErrMalformedPayload
	}
	groupID := string(data[pos : pos+groupLen])
	pos += groupLen

	topicLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+topicLen+4 {
		return OffsetReq{}, ErrMalformedPayload
	}
	topic := string(data[pos : pos+topicLen])
	pos += topicLen

	partitionID := binary.BigEndian.Uint32(data[pos : pos+4])

	return OffsetReq{
		GroupID:     groupID,
		Topic:       topic,
		PartitionID: partitionID,
	}, nil
}

// EncodeOffsetResp serializes an OffsetResp into a binary payload.
func EncodeOffsetResp(resp OffsetResp) []byte {
	buf := make([]byte, 10)
	binary.BigEndian.PutUint16(buf[0:2], resp.Status)
	binary.BigEndian.PutUint64(buf[2:10], resp.Offset)
	return buf
}

// DecodeOffsetResp deserializes binary data into an OffsetResp.
func DecodeOffsetResp(data []byte) (OffsetResp, error) {
	if len(data) < 10 {
		return OffsetResp{}, ErrMalformedPayload
	}
	return OffsetResp{
		Status: binary.BigEndian.Uint16(data[0:2]),
		Offset: binary.BigEndian.Uint64(data[2:10]),
	}, nil
}

// EncodeCommitReq serializes a CommitReq into a binary payload.
func EncodeCommitReq(req CommitReq) []byte {
	groupBytes := clampBytes([]byte(req.GroupID))
	topicBytes := clampBytes([]byte(req.Topic))
	metaBytes := clampBytes([]byte(req.Metadata))

	totalLen := 2 + len(groupBytes) + 2 + len(topicBytes) + 4 + 8 + 2 + len(metaBytes)
	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint16(buf[0:2], uint16(len(groupBytes)))
	copy(buf[2:2+len(groupBytes)], groupBytes)
	pos := 2 + len(groupBytes)

	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(topicBytes)))
	pos += 2
	copy(buf[pos:pos+len(topicBytes)], topicBytes)
	pos += len(topicBytes)

	binary.BigEndian.PutUint32(buf[pos:pos+4], req.PartitionID)
	pos += 4

	binary.BigEndian.PutUint64(buf[pos:pos+8], req.Offset)
	pos += 8

	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(metaBytes)))
	pos += 2
	copy(buf[pos:pos+len(metaBytes)], metaBytes)

	return buf
}

// DecodeCommitReq deserializes binary data into a CommitReq.
func DecodeCommitReq(data []byte) (CommitReq, error) {
	if len(data) < 18 {
		return CommitReq{}, ErrMalformedPayload
	}

	groupLen := int(binary.BigEndian.Uint16(data[0:2]))
	pos := 2
	if len(data) < pos+groupLen+16 {
		return CommitReq{}, ErrMalformedPayload
	}
	groupID := string(data[pos : pos+groupLen])
	pos += groupLen

	topicLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+topicLen+14 {
		return CommitReq{}, ErrMalformedPayload
	}
	topic := string(data[pos : pos+topicLen])
	pos += topicLen

	partitionID := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4

	offset := binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8

	metaLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+metaLen {
		return CommitReq{}, ErrMalformedPayload
	}
	metadata := string(data[pos : pos+metaLen])

	return CommitReq{
		GroupID:     groupID,
		Topic:       topic,
		PartitionID: partitionID,
		Offset:      offset,
		Metadata:    metadata,
	}, nil
}

// EncodeCommitResp serializes a CommitResp into a binary payload.
func EncodeCommitResp(resp CommitResp) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf[0:2], resp.Status)
	return buf
}

// DecodeCommitResp deserializes binary data into a CommitResp.
func DecodeCommitResp(data []byte) (CommitResp, error) {
	if len(data) < 2 {
		return CommitResp{}, ErrMalformedPayload
	}
	return CommitResp{
		Status: binary.BigEndian.Uint16(data[0:2]),
	}, nil
}

// EncodePingReq serializes a PingReq into a binary payload.
func EncodePingReq(req PingReq) []byte {
	if len(req.Echo) == 0 {
		return nil
	}
	buf := make([]byte, len(req.Echo))
	copy(buf, req.Echo)
	return buf
}

// DecodePingReq deserializes binary data into a PingReq.
func DecodePingReq(data []byte) (PingReq, error) {
	return PingReq{Echo: data}, nil
}

// EncodePongResp serializes a PongResp into a binary payload.
func EncodePongResp(resp PongResp) []byte {
	if len(resp.Echo) == 0 {
		return nil
	}
	buf := make([]byte, len(resp.Echo))
	copy(buf, resp.Echo)
	return buf
}

// DecodePongResp deserializes binary data into a PongResp.
func DecodePongResp(data []byte) (PongResp, error) {
	return PongResp{Echo: data}, nil
}
