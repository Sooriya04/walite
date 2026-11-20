// Package: test
// File: record_test.go
// Core Work: Unit, fuzzing, and bit-flip integrity tests for the binary Record codec.
// Exported: TestRecord_Roundtrip, TestRecord_BitFlipCorruption, TestRecord_TruncatedSafety, BenchmarkRecord_Encode

package test

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/barbaravane/walite/internal/log"
)

func TestRecord_Roundtrip(t *testing.T) {
	tests := []struct {
		name  string
		rec   *log.Record
		isKey bool
	}{
		{
			name: "Keyless record with small payload",
			rec: &log.Record{
				Offset:    0,
				Timestamp: time.Now().UnixNano(),
				Key:       nil,
				Value:     []byte("hello walite!"),
			},
			isKey: false,
		},
		{
			name: "Keyed record with binary payload",
			rec: &log.Record{
				Offset:    1048576,
				Timestamp: time.Now().UnixNano(),
				Key:       []byte("sensor-alpha-09"),
				Value:     []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04},
			},
			isKey: true,
		},
		{
			name: "Empty payload record",
			rec: &log.Record{
				Offset:    42,
				Timestamp: 1700000000000,
				Key:       nil,
				Value:     nil,
			},
			isKey: false,
		},
	}

	buf := make([]byte, 512)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded := log.EncodeRecord(buf, tc.rec)
			if len(encoded) < log.HeaderSize {
				t.Fatalf("encoded length %d is less than HeaderSize %d", len(encoded), log.HeaderSize)
			}

			decoded, n, err := log.DecodeRecord(encoded)
			if err != nil {
				t.Fatalf("failed to decode valid record: %v", err)
			}
			if n != len(encoded) {
				t.Fatalf("consumed bytes %d != encoded length %d", n, len(encoded))
			}

			if decoded.Offset != tc.rec.Offset {
				t.Errorf("expected offset %d, got %d", tc.rec.Offset, decoded.Offset)
			}
			if decoded.Timestamp != tc.rec.Timestamp {
				t.Errorf("expected timestamp %d, got %d", tc.rec.Timestamp, decoded.Timestamp)
			}
			if !bytes.Equal(decoded.Key, tc.rec.Key) {
				t.Errorf("key mismatch: expected %v, got %v", tc.rec.Key, decoded.Key)
			}
			if !bytes.Equal(decoded.Value, tc.rec.Value) {
				t.Errorf("value mismatch: expected %v, got %v", tc.rec.Value, decoded.Value)
			}
		})
	}
}

func TestRecord_BitFlipCorruption(t *testing.T) {
	rec := &log.Record{
		Offset:    999,
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("partition-key"),
		Value:     []byte("critical financial transaction payload"),
	}

	encoded := log.EncodeRecord(nil, rec)

	// Flip 1 bit across every single byte in the serialized stream
	for i := 0; i < len(encoded); i++ {
		corrupted := make([]byte, len(encoded))
		copy(corrupted, encoded)

		// Invert bits on byte i
		corrupted[i] ^= 0xFF

		_, _, err := log.DecodeRecord(corrupted)
		// If keyLen or valLen byte was corrupted to exceed buffer capacity, ErrTruncatedRecord or ErrRecordTooLarge is valid;
		// otherwise, CRC mismatch produces ErrCorruptRecord. None should ever succeed.
		if err != log.ErrCorruptRecord && err != log.ErrTruncatedRecord && err != log.ErrRecordTooLarge {
			t.Fatalf("byte %d was corrupted but DecodeRecord returned unexpected err: %v", i, err)
		}
	}
}

func TestRecord_TruncatedSafety(t *testing.T) {
	rec := &log.Record{
		Offset:    1,
		Timestamp: 123456789,
		Key:       []byte("key"),
		Value:     []byte("value payload"),
	}

	encoded := log.EncodeRecord(nil, rec)

	// Slicing from 0 bytes up to len(encoded)-1 must fail safely without panics
	for i := 0; i < len(encoded); i++ {
		truncated := encoded[:i]
		_, _, err := log.DecodeRecord(truncated)
		if err != log.ErrTruncatedRecord {
			t.Fatalf("truncated slice of length %d should return ErrTruncatedRecord, got: %v", i, err)
		}
	}
}

func TestRecord_PayloadLimit(t *testing.T) {
	header := make([]byte, log.HeaderSize)
	binary.BigEndian.PutUint32(header[20:24], 1024)
	binary.BigEndian.PutUint32(header[24:28], uint32(log.MaxRecordPayload))

	// Total exceeds MaxRecordPayload
	_, _, err := log.ParseHeaderLengths(header)
	if err != log.ErrRecordTooLarge {
		t.Fatalf("expected ErrRecordTooLarge from ParseHeaderLengths, got %v", err)
	}

	src := make([]byte, log.HeaderSize+16)
	binary.BigEndian.PutUint32(src[20:24], 0)
	binary.BigEndian.PutUint32(src[24:28], uint32(log.MaxRecordPayload+1))
	_, _, err = log.DecodeRecord(src)
	if err != log.ErrRecordTooLarge {
		t.Fatalf("expected ErrRecordTooLarge from DecodeRecord, got %v", err)
	}
}

func BenchmarkRecord_Encode(b *testing.B) {
	rec := &log.Record{
		Offset:    1000,
		Timestamp: 1700000000,
		Key:       []byte("metric-key"),
		Value:     []byte("metric value payload data points"),
	}
	buf := make([]byte, 128)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = log.EncodeRecord(buf, rec)
	}
}
