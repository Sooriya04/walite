// Package: test
// File: protocol_test.go
// Core Work: Comprehensive test suite for wire protocol binary framing, roundtrip message serialization, and corruption rejection.
// Exported: none (test functions)
package test

/*
Functions Manifest & Usage:
- TestProtocol_FrameRoundtrip(t *testing.T): Validates encoding and decoding of frames with various payload sizes.
- TestProtocol_InvalidMagic(t *testing.T): Ensures frames with wrong magic bytes are rejected with ErrInvalidMagic.
- TestProtocol_UnsupportedVersion(t *testing.T): Ensures unsupported protocol versions return ErrUnsupportedVersion.
- TestProtocol_FrameTooLarge(t *testing.T): Verifies that frames exceeding 16MB are rejected with ErrFrameTooLarge.
- TestProtocol_TruncatedFrame(t *testing.T): Asserts ErrTruncatedFrame on severed streams and io.EOF on clean disconnects.
- TestProtocol_ProduceRoundtrip(t *testing.T): Verifies binary serialization and recovery of ProduceReq and ProduceResp.
- TestProtocol_ConsumeRoundtrip(t *testing.T): Verifies binary serialization and recovery of ConsumeReq and ConsumeResp.
- TestProtocol_OffsetRoundtrip(t *testing.T): Verifies binary serialization and recovery of OffsetReq and OffsetResp.
- TestProtocol_CommitRoundtrip(t *testing.T): Verifies binary serialization and recovery of CommitReq and CommitResp.
- TestProtocol_OversizedStringClamping(t *testing.T): Asserts that strings exceeding 65,535 bytes are clamped without 16-bit integer overflow.
- TestProtocol_PingPongRoundtrip(t *testing.T): Verifies heartbeat echo serialization.
- TestProtocol_MalformedPayloads(t *testing.T): Tests bounds checks and error handling for malformed/truncated payloads.
- TestProtocol_NetPipeTransport(t *testing.T): Validates duplex stream communication over a simulated network connection.
- BenchmarkProtocol_EncodeFrame(b *testing.B): Measures framing serialization latency and heap allocations.
- BenchmarkProtocol_ReadFrame(b *testing.B): Measures framing deserialization latency and heap allocations.
*/

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/barbaravane/walite/internal/log"
	"github.com/barbaravane/walite/internal/protocol"
)

func TestProtocol_FrameRoundtrip(t *testing.T) {
	testCases := []struct {
		name    string
		opCode  uint8
		payload []byte
	}{
		{"EmptyPayload", protocol.OpPingReq, nil},
		{"SmallPayload", protocol.OpProduceResp, []byte("ok")},
		{"BinaryPayload", protocol.OpConsumeResp, bytes.Repeat([]byte{0xDE, 0xAD, 0xBE, 0xEF}, 256)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := protocol.WriteFrame(&buf, tc.opCode, tc.payload); err != nil {
				t.Fatalf("WriteFrame failed: %v", err)
			}

			frame, err := protocol.ReadFrame(&buf)
			if err != nil {
				t.Fatalf("ReadFrame failed: %v", err)
			}

			if frame.OpCode != tc.opCode {
				t.Fatalf("expected opCode %x, got %x", tc.opCode, frame.OpCode)
			}

			if len(tc.payload) == 0 {
				if len(frame.Payload) != 0 {
					t.Fatalf("expected empty payload, got %d bytes", len(frame.Payload))
				}
			} else if !bytes.Equal(frame.Payload, tc.payload) {
				t.Fatalf("payload mismatch")
			}
		})
	}
}

func TestProtocol_InvalidMagic(t *testing.T) {
	raw := protocol.EncodeFrame(protocol.OpPingReq, []byte("test"))
	// Corrupt magic bytes
	raw[4] = 0x00
	raw[5] = 0x00

	buf := bytes.NewReader(raw)
	_, err := protocol.ReadFrame(buf)
	if !errors.Is(err, protocol.ErrInvalidMagic) {
		t.Fatalf("expected ErrInvalidMagic, got %v", err)
	}
}

func TestProtocol_UnsupportedVersion(t *testing.T) {
	raw := protocol.EncodeFrame(protocol.OpPingReq, []byte("test"))
	// Change version to 0x02
	raw[6] = 0x02

	buf := bytes.NewReader(raw)
	_, err := protocol.ReadFrame(buf)
	if !errors.Is(err, protocol.ErrUnsupportedVersion) {
		t.Fatalf("expected ErrUnsupportedVersion, got %v", err)
	}
}

func TestProtocol_FrameTooLarge(t *testing.T) {
	raw := protocol.EncodeFrame(protocol.OpPingReq, nil)
	// Set FrameLen to MaxFrameBytes + 1
	binary.BigEndian.PutUint32(raw[0:4], protocol.MaxFrameBytes+1)

	buf := bytes.NewReader(raw)
	_, err := protocol.ReadFrame(buf)
	if !errors.Is(err, protocol.ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge, got %v", err)
	}
}

func TestProtocol_TruncatedFrame(t *testing.T) {
	t.Run("TruncatedPayloadMidway", func(t *testing.T) {
		raw := protocol.EncodeFrame(protocol.OpProduceReq, []byte("full payload data here"))
		// Truncate halfway through payload (io.ErrUnexpectedEOF from io.ReadFull)
		truncated := raw[:len(raw)-5]

		buf := bytes.NewReader(truncated)
		_, err := protocol.ReadFrame(buf)
		if err == nil {
			t.Fatalf("expected error reading truncated frame, got nil")
		}
		if !errors.Is(err, protocol.ErrTruncatedFrame) {
			t.Fatalf("expected ErrTruncatedFrame, got %v", err)
		}
	})

	t.Run("TruncatedPayloadZeroBytes", func(t *testing.T) {
		raw := protocol.EncodeFrame(protocol.OpProduceReq, []byte("full payload data here"))
		// Truncate exactly after header, so 0 payload bytes are read (io.EOF from io.ReadFull)
		truncated := raw[:protocol.HeaderSize]

		buf := bytes.NewReader(truncated)
		_, err := protocol.ReadFrame(buf)
		if err == nil {
			t.Fatalf("expected error reading truncated frame, got nil")
		}
		if !errors.Is(err, protocol.ErrTruncatedFrame) {
			t.Fatalf("expected ErrTruncatedFrame, got %v", err)
		}
	})

	t.Run("TruncatedHeaderMidway", func(t *testing.T) {
		raw := protocol.EncodeFrame(protocol.OpProduceReq, []byte("full payload data here"))
		// Truncate halfway through header (e.g. 3 bytes out of 8)
		truncated := raw[:3]

		buf := bytes.NewReader(truncated)
		_, err := protocol.ReadFrame(buf)
		if err == nil {
			t.Fatalf("expected error reading truncated header, got nil")
		}
		if !errors.Is(err, protocol.ErrTruncatedFrame) {
			t.Fatalf("expected ErrTruncatedFrame, got %v", err)
		}
	})

	t.Run("CleanEOFOnHeader", func(t *testing.T) {
		// Zero bytes in reader -> clean EOF
		buf := bytes.NewReader(nil)
		_, err := protocol.ReadFrame(buf)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected io.EOF on clean stream close, got %v", err)
		}
	})
}

func TestProtocol_ProduceRoundtrip(t *testing.T) {
	rec1 := &log.Record{
		Offset:    0,
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("order_id"),
		Value:     []byte("12345"),
	}
	rec2 := &log.Record{
		Offset:    0,
		Timestamp: time.Now().UnixNano(),
		Key:       nil,
		Value:     []byte("keyless_event"),
	}

	req := protocol.ProduceReq{
		Topic:       "telemetry-events",
		PartitionID: 3,
		Records:     []*log.Record{rec1, rec2},
	}

	encodedReq := protocol.EncodeProduceReq(req)
	decodedReq, err := protocol.DecodeProduceReq(encodedReq)
	if err != nil {
		t.Fatalf("DecodeProduceReq failed: %v", err)
	}

	if decodedReq.Topic != req.Topic || decodedReq.PartitionID != req.PartitionID {
		t.Fatalf("produce req header mismatch")
	}
	if len(decodedReq.Records) != len(req.Records) {
		t.Fatalf("expected %d records, got %d", len(req.Records), len(decodedReq.Records))
	}
	if !bytes.Equal(decodedReq.Records[0].Key, rec1.Key) || !bytes.Equal(decodedReq.Records[0].Value, rec1.Value) {
		t.Fatalf("record 0 payload mismatch")
	}
	if len(decodedReq.Records[1].Key) != 0 || !bytes.Equal(decodedReq.Records[1].Value, rec2.Value) {
		t.Fatalf("record 1 payload mismatch")
	}

	resp := protocol.ProduceResp{
		Status:     protocol.StatusOK,
		BaseOffset: 1042,
		Count:      2,
	}
	encodedResp := protocol.EncodeProduceResp(resp)
	decodedResp, err := protocol.DecodeProduceResp(encodedResp)
	if err != nil {
		t.Fatalf("DecodeProduceResp failed: %v", err)
	}
	if !reflect.DeepEqual(resp, decodedResp) {
		t.Fatalf("produce resp mismatch: expected %+v, got %+v", resp, decodedResp)
	}
}

func TestProtocol_ConsumeRoundtrip(t *testing.T) {
	req := protocol.ConsumeReq{
		Topic:       "metrics",
		PartitionID: 0,
		Offset:      500,
		MaxBytes:    65536,
	}
	encodedReq := protocol.EncodeConsumeReq(req)
	decodedReq, err := protocol.DecodeConsumeReq(encodedReq)
	if err != nil {
		t.Fatalf("DecodeConsumeReq failed: %v", err)
	}
	if !reflect.DeepEqual(req, decodedReq) {
		t.Fatalf("consume req mismatch: expected %+v, got %+v", req, decodedReq)
	}

	rec := &log.Record{
		Offset:    500,
		Timestamp: 1000000,
		Key:       []byte("cpu"),
		Value:     []byte("85.4"),
	}
	resp := protocol.ConsumeResp{
		Status:  protocol.StatusOK,
		Records: []*log.Record{rec},
	}
	encodedResp := protocol.EncodeConsumeResp(resp)
	decodedResp, err := protocol.DecodeConsumeResp(encodedResp)
	if err != nil {
		t.Fatalf("DecodeConsumeResp failed: %v", err)
	}
	if decodedResp.Status != resp.Status || len(decodedResp.Records) != 1 {
		t.Fatalf("consume resp mismatch")
	}
	if !bytes.Equal(decodedResp.Records[0].Value, rec.Value) {
		t.Fatalf("record value mismatch")
	}
}

func TestProtocol_OffsetRoundtrip(t *testing.T) {
	req := protocol.OffsetReq{
		GroupID:     "analytics-workers",
		Topic:       "orders",
		PartitionID: 2,
	}
	encodedReq := protocol.EncodeOffsetReq(req)
	decodedReq, err := protocol.DecodeOffsetReq(encodedReq)
	if err != nil {
		t.Fatalf("DecodeOffsetReq failed: %v", err)
	}
	if !reflect.DeepEqual(req, decodedReq) {
		t.Fatalf("offset req mismatch: expected %+v, got %+v", req, decodedReq)
	}

	resp := protocol.OffsetResp{
		Status: protocol.StatusOK,
		Offset: 9942,
	}
	encodedResp := protocol.EncodeOffsetResp(resp)
	decodedResp, err := protocol.DecodeOffsetResp(encodedResp)
	if err != nil {
		t.Fatalf("DecodeOffsetResp failed: %v", err)
	}
	if !reflect.DeepEqual(resp, decodedResp) {
		t.Fatalf("offset resp mismatch")
	}
}

func TestProtocol_CommitRoundtrip(t *testing.T) {
	req := protocol.CommitReq{
		GroupID:     "payments-group",
		Topic:       "transactions",
		PartitionID: 1,
		Offset:      15430,
		Metadata:    "checkpointed-worker-03",
	}
	encodedReq := protocol.EncodeCommitReq(req)
	decodedReq, err := protocol.DecodeCommitReq(encodedReq)
	if err != nil {
		t.Fatalf("DecodeCommitReq failed: %v", err)
	}
	if !reflect.DeepEqual(req, decodedReq) {
		t.Fatalf("commit req mismatch: expected %+v, got %+v", req, decodedReq)
	}

	resp := protocol.CommitResp{
		Status: protocol.StatusOK,
	}
	encodedResp := protocol.EncodeCommitResp(resp)
	decodedResp, err := protocol.DecodeCommitResp(encodedResp)
	if err != nil {
		t.Fatalf("DecodeCommitResp failed: %v", err)
	}
	if !reflect.DeepEqual(resp, decodedResp) {
		t.Fatalf("commit resp mismatch")
	}
}

func TestProtocol_OversizedStringClamping(t *testing.T) {
	hugeString := strings.Repeat("A", 70000)

	t.Run("CommitReq_MetadataClamped", func(t *testing.T) {
		req := protocol.CommitReq{
			GroupID:     "group",
			Topic:       "topic",
			PartitionID: 0,
			Offset:      100,
			Metadata:    hugeString,
		}
		encoded := protocol.EncodeCommitReq(req)
		decoded, err := protocol.DecodeCommitReq(encoded)
		if err != nil {
			t.Fatalf("DecodeCommitReq failed: %v", err)
		}
		if len(decoded.Metadata) != 65535 {
			t.Fatalf("expected Metadata len 65535, got %d", len(decoded.Metadata))
		}
		if decoded.Metadata != hugeString[:65535] {
			t.Fatalf("metadata content mismatch")
		}
	})

	t.Run("CommitReq_GroupIDClamped", func(t *testing.T) {
		req := protocol.CommitReq{
			GroupID:     hugeString,
			Topic:       "topic",
			PartitionID: 0,
			Offset:      100,
			Metadata:    "meta",
		}
		encoded := protocol.EncodeCommitReq(req)
		decoded, err := protocol.DecodeCommitReq(encoded)
		if err != nil {
			t.Fatalf("DecodeCommitReq failed: %v", err)
		}
		if len(decoded.GroupID) != 65535 {
			t.Fatalf("expected GroupID len 65535, got %d", len(decoded.GroupID))
		}
	})

	t.Run("ProduceReq_TopicClamped", func(t *testing.T) {
		req := protocol.ProduceReq{
			Topic:       hugeString,
			PartitionID: 0,
			Records: []*log.Record{
				{Value: []byte("val")},
			},
		}
		encoded := protocol.EncodeProduceReq(req)
		decoded, err := protocol.DecodeProduceReq(encoded)
		if err != nil {
			t.Fatalf("DecodeProduceReq failed: %v", err)
		}
		if len(decoded.Topic) != 65535 {
			t.Fatalf("expected Topic len 65535, got %d", len(decoded.Topic))
		}
	})

	t.Run("ConsumeReq_TopicClamped", func(t *testing.T) {
		req := protocol.ConsumeReq{
			Topic:       hugeString,
			PartitionID: 0,
			Offset:      0,
			MaxBytes:    1024,
		}
		encoded := protocol.EncodeConsumeReq(req)
		decoded, err := protocol.DecodeConsumeReq(encoded)
		if err != nil {
			t.Fatalf("DecodeConsumeReq failed: %v", err)
		}
		if len(decoded.Topic) != 65535 {
			t.Fatalf("expected Topic len 65535, got %d", len(decoded.Topic))
		}
	})

	t.Run("OffsetReq_TopicClamped", func(t *testing.T) {
		req := protocol.OffsetReq{
			GroupID:     "group",
			Topic:       hugeString,
			PartitionID: 0,
		}
		encoded := protocol.EncodeOffsetReq(req)
		decoded, err := protocol.DecodeOffsetReq(encoded)
		if err != nil {
			t.Fatalf("DecodeOffsetReq failed: %v", err)
		}
		if len(decoded.Topic) != 65535 {
			t.Fatalf("expected Topic len 65535, got %d", len(decoded.Topic))
		}
	})
}

func TestProtocol_PingPongRoundtrip(t *testing.T) {
	ping := protocol.PingReq{Echo: []byte("heartbeat-data")}
	encPing := protocol.EncodePingReq(ping)
	decPing, err := protocol.DecodePingReq(encPing)
	if err != nil {
		t.Fatalf("DecodePingReq failed: %v", err)
	}
	if !bytes.Equal(ping.Echo, decPing.Echo) {
		t.Fatalf("ping echo mismatch")
	}

	pong := protocol.PongResp{Echo: []byte("heartbeat-ack")}
	encPong := protocol.EncodePongResp(pong)
	decPong, err := protocol.DecodePongResp(encPong)
	if err != nil {
		t.Fatalf("DecodePongResp failed: %v", err)
	}
	if !bytes.Equal(pong.Echo, decPong.Echo) {
		t.Fatalf("pong echo mismatch")
	}
}

func TestProtocol_MalformedPayloads(t *testing.T) {
	shortData := []byte{0x00, 0x01}

	if _, err := protocol.DecodeProduceReq(shortData); !errors.Is(err, protocol.ErrMalformedPayload) {
		t.Fatalf("expected ErrMalformedPayload for short produce req, got %v", err)
	}
	if _, err := protocol.DecodeProduceResp(shortData); !errors.Is(err, protocol.ErrMalformedPayload) {
		t.Fatalf("expected ErrMalformedPayload for short produce resp, got %v", err)
	}
	if _, err := protocol.DecodeConsumeReq(shortData); !errors.Is(err, protocol.ErrMalformedPayload) {
		t.Fatalf("expected ErrMalformedPayload for short consume req, got %v", err)
	}
	if _, err := protocol.DecodeConsumeResp(shortData); !errors.Is(err, protocol.ErrMalformedPayload) {
		t.Fatalf("expected ErrMalformedPayload for short consume resp, got %v", err)
	}
	if _, err := protocol.DecodeOffsetReq(shortData); !errors.Is(err, protocol.ErrMalformedPayload) {
		t.Fatalf("expected ErrMalformedPayload for short offset req, got %v", err)
	}
	if _, err := protocol.DecodeOffsetResp(shortData); !errors.Is(err, protocol.ErrMalformedPayload) {
		t.Fatalf("expected ErrMalformedPayload for short offset resp, got %v", err)
	}
	if _, err := protocol.DecodeCommitReq(shortData); !errors.Is(err, protocol.ErrMalformedPayload) {
		t.Fatalf("expected ErrMalformedPayload for short commit req, got %v", err)
	}
	if _, err := protocol.DecodeCommitResp(nil); !errors.Is(err, protocol.ErrMalformedPayload) {
		t.Fatalf("expected ErrMalformedPayload for empty commit resp, got %v", err)
	}
}

func TestProtocol_NetPipeTransport(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	done := make(chan struct{})

	// Server goroutine: echo back received frames with OpPongResp
	go func() {
		defer close(done)
		frame, err := protocol.ReadFrame(serverConn)
		if err != nil {
			t.Errorf("server ReadFrame error: %v", err)
			return
		}
		if frame.OpCode != protocol.OpPingReq {
			t.Errorf("expected OpPingReq, got %x", frame.OpCode)
			return
		}
		if err := protocol.WriteFrame(serverConn, protocol.OpPongResp, frame.Payload); err != nil {
			t.Errorf("server WriteFrame error: %v", err)
			return
		}
	}()

	payload := []byte("duplex-network-ping")
	if err := protocol.WriteFrame(clientConn, protocol.OpPingReq, payload); err != nil {
		t.Fatalf("client WriteFrame error: %v", err)
	}

	resp, err := protocol.ReadFrame(clientConn)
	if err != nil {
		t.Fatalf("client ReadFrame error: %v", err)
	}
	if resp.OpCode != protocol.OpPongResp || !bytes.Equal(resp.Payload, payload) {
		t.Fatalf("unexpected pong response: %+v", resp)
	}

	<-done
}

func BenchmarkProtocol_EncodeFrame(b *testing.B) {
	payload := bytes.Repeat([]byte{0x42}, 1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = protocol.EncodeFrame(protocol.OpProduceReq, payload)
	}
}

func BenchmarkProtocol_ReadFrame(b *testing.B) {
	payload := bytes.Repeat([]byte{0x42}, 1024)
	raw := protocol.EncodeFrame(protocol.OpProduceReq, payload)
	reader := bytes.NewReader(raw)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		reader.Reset(raw)
		_, _ = protocol.ReadFrame(reader)
	}
}
