// Package: log
// File: record.go
// Core Work: Defines the in-memory Record representation, zero-allocation binary wire serialization, and header inspection.
// Exported: Record, HeaderSize, MaxRecordPayload, EncodeRecord, DecodeRecord, ParseHeaderLengths, ErrCorruptRecord, ErrTruncatedRecord, ErrRecordTooLarge

package log

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var (
	// ErrCorruptRecord is returned when the decoded CRC32 does not match the payload checksum.
	ErrCorruptRecord = errors.New("walite: corrupt record checksum mismatch")

	// ErrTruncatedRecord is returned when the byte slice is shorter than the minimum header or declared length.
	ErrTruncatedRecord = errors.New("walite: truncated record bytes")

	// ErrRecordTooLarge is returned when key or value length exceeds MaxRecordPayload.
	ErrRecordTooLarge = errors.New("walite: record payload exceeds maximum limit of 64MB")
)

// HeaderSize is the fixed byte overhead of a record on disk:
// Offset (8B) + Timestamp (8B) + CRC32 (4B) + KeyLen (4B) + ValLen (4B) = 28 Bytes.
const HeaderSize = 28

// MaxRecordPayload defines the maximum allowable size (64MB) for a record's key and value combined.
// This bounds memory allocation during serialization and recovery, preventing adversarial OOM crashes.
const MaxRecordPayload = 64 * 1024 * 1024

// ParseHeaderLengths extracts keyLen and valLen from a 28-byte header slice without decoding the full record.
func ParseHeaderLengths(header []byte) (int, int, error) {
	if len(header) < HeaderSize {
		return 0, 0, ErrTruncatedRecord
	}
	keyLen := int(binary.BigEndian.Uint32(header[20:24]))
	valLen := int(binary.BigEndian.Uint32(header[24:28]))
	if uint64(keyLen)+uint64(valLen) > MaxRecordPayload {
		return 0, 0, ErrRecordTooLarge
	}
	return keyLen, valLen, nil
}

// Record represents a single append-only commit unit in a partition.
type Record struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// EncodeRecord serializes a Record into the destination buffer without heap allocations.
// Binary Layout:
// [Offset: 8B uint64][Timestamp: 8B int64][CRC32: 4B uint32][KeyLen: 4B uint32][ValLen: 4B uint32][Key][Value]
func EncodeRecord(dst []byte, r *Record) []byte {
	keyLen := len(r.Key)
	valLen := len(r.Value)
	totalLen := HeaderSize + keyLen + valLen

	// Grow or allocate dst slice if necessary
	if cap(dst) >= totalLen {
		dst = dst[:totalLen]
	} else {
		dst = make([]byte, totalLen)
	}

	// Write Offset and Timestamp
	binary.BigEndian.PutUint64(dst[0:8], r.Offset)
	binary.BigEndian.PutUint64(dst[8:16], uint64(r.Timestamp))

	// Write KeyLen and ValLen
	binary.BigEndian.PutUint32(dst[20:24], uint32(keyLen))
	binary.BigEndian.PutUint32(dst[24:28], uint32(valLen))

	// Copy Key and Value payloads
	if keyLen > 0 {
		copy(dst[HeaderSize:HeaderSize+keyLen], r.Key)
	}
	if valLen > 0 {
		copy(dst[HeaderSize+keyLen:totalLen], r.Value)
	}

	// Compute CRC32 over Offset(8) + Timestamp(8) + KeyLen(4) + ValLen(4) + Key + Value
	c := crc32.ChecksumIEEE(dst[0:16])
	checksum := crc32.Update(c, crc32.IEEETable, dst[20:totalLen])

	// Store CRC32 in bytes 16..20
	binary.BigEndian.PutUint32(dst[16:20], checksum)

	return dst
}

// DecodeRecord parses a serialized binary record, validates its CRC32 checksum,
// and returns the constructed Record along with the total bytes consumed.
func DecodeRecord(src []byte) (*Record, int, error) {
	if len(src) < HeaderSize {
		return nil, 0, ErrTruncatedRecord
	}

	offset := binary.BigEndian.Uint64(src[0:8])
	timestamp := int64(binary.BigEndian.Uint64(src[8:16]))
	expectedChecksum := binary.BigEndian.Uint32(src[16:20])
	keyLen := int(binary.BigEndian.Uint32(src[20:24]))
	valLen := int(binary.BigEndian.Uint32(src[24:28]))

	if uint64(keyLen)+uint64(valLen) > MaxRecordPayload {
		return nil, 0, ErrRecordTooLarge
	}

	totalLen := HeaderSize + keyLen + valLen
	if len(src) < totalLen {
		return nil, 0, ErrTruncatedRecord
	}

	// Verify CRC32 checksum integrity
	c := crc32.ChecksumIEEE(src[0:16])
	actualChecksum := crc32.Update(c, crc32.IEEETable, src[20:totalLen])

	if actualChecksum != expectedChecksum {
		return nil, 0, ErrCorruptRecord
	}

	var key []byte
	if keyLen > 0 {
		key = make([]byte, keyLen)
		copy(key, src[HeaderSize:HeaderSize+keyLen])
	}

	var val []byte
	if valLen > 0 {
		val = make([]byte, valLen)
		copy(val, src[HeaderSize+keyLen:totalLen])
	}

	rec := &Record{
		Offset:    offset,
		Timestamp: timestamp,
		Key:       key,
		Value:     val,
	}

	return rec, totalLen, nil
}
