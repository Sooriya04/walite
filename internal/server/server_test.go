// Package: server
// File: server_test.go
// Core Work: Unit and integration tests for walited TCP server engine lifecycle and stream operations.
// Exported: none (test functions)
package server

/*
Functions Manifest & Usage:
- TestServer_StartStop(t *testing.T): Tests server startup, ping-pong roundtrip, and clean shutdown.
- TestServer_ProduceAndConsume(t *testing.T): Tests network produce batch and consume roundtrip.
- TestServer_OffsetCommitAndGet(t *testing.T): Tests consumer offset commit and retrieval over TCP.
- TestServer_PartitionNotFound(t *testing.T): Verifies error response on consuming from uncreated partition.
*/

import (
	"bytes"
	"net"
	"os"
	"testing"
	"time"

	"github.com/barbaravane/walite/internal/log"
	"github.com/barbaravane/walite/internal/protocol"
)

func setupTestServer(t *testing.T) (*Server, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "walite-server-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg := Config{
		Addr:               "127.0.0.1:0", // Ephemeral port
		DataDir:            dir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 1024,
		FlushInterval:      100 * time.Millisecond,
		SyncAlways:         false,
		ReadTimeout:        5 * time.Second,
		WriteTimeout:       5 * time.Second,
	}

	srv, err := NewServer(cfg)
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatalf("failed to create server: %v", err)
	}

	if err := srv.Start(); err != nil {
		_ = os.RemoveAll(dir)
		t.Fatalf("failed to start server: %v", err)
	}

	cleanup := func() {
		_ = srv.Close()
		_ = os.RemoveAll(dir)
	}

	return srv, cleanup
}

func TestServer_StartStop(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Send Ping
	pingPayload := protocol.EncodePingReq(protocol.PingReq{Echo: []byte("server-ping")})
	if err := protocol.WriteFrame(conn, protocol.OpPingReq, pingPayload); err != nil {
		t.Fatalf("write ping failed: %v", err)
	}

	respFrame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read pong failed: %v", err)
	}
	if respFrame.OpCode != protocol.OpPongResp {
		t.Fatalf("expected OpPongResp, got %x", respFrame.OpCode)
	}

	pong, err := protocol.DecodePongResp(respFrame.Payload)
	if err != nil {
		t.Fatalf("decode pong failed: %v", err)
	}
	if !bytes.Equal(pong.Echo, []byte("server-ping")) {
		t.Fatalf("pong payload mismatch")
	}
}

func TestServer_ProduceAndConsume(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	rec1 := &log.Record{
		Offset:    0,
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("k1"),
		Value:     []byte("value-alpha"),
	}
	rec2 := &log.Record{
		Offset:    0,
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("k2"),
		Value:     []byte("value-beta"),
	}

	// 1. Produce batch
	produceReq := protocol.ProduceReq{
		Topic:       "events",
		PartitionID: 0,
		Records:     []*log.Record{rec1, rec2},
	}
	payload := protocol.EncodeProduceReq(produceReq)
	if err := protocol.WriteFrame(conn, protocol.OpProduceReq, payload); err != nil {
		t.Fatalf("write produce failed: %v", err)
	}

	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read produce resp failed: %v", err)
	}
	if frame.OpCode != protocol.OpProduceResp {
		t.Fatalf("expected OpProduceResp, got %x", frame.OpCode)
	}
	produceResp, err := protocol.DecodeProduceResp(frame.Payload)
	if err != nil {
		t.Fatalf("decode produce resp failed: %v", err)
	}
	if produceResp.Status != protocol.StatusOK {
		t.Fatalf("expected StatusOK, got %d", produceResp.Status)
	}
	if produceResp.BaseOffset != 0 || produceResp.Count != 2 {
		t.Fatalf("unexpected produce resp: %+v", produceResp)
	}

	// 2. Consume back
	consumeReq := protocol.ConsumeReq{
		Topic:       "events",
		PartitionID: 0,
		Offset:      0,
		MaxBytes:    65536,
	}
	payload = protocol.EncodeConsumeReq(consumeReq)
	if err := protocol.WriteFrame(conn, protocol.OpConsumeReq, payload); err != nil {
		t.Fatalf("write consume failed: %v", err)
	}

	frame, err = protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read consume resp failed: %v", err)
	}
	if frame.OpCode != protocol.OpConsumeResp {
		t.Fatalf("expected OpConsumeResp, got %x", frame.OpCode)
	}
	consumeResp, err := protocol.DecodeConsumeResp(frame.Payload)
	if err != nil {
		t.Fatalf("decode consume resp failed: %v", err)
	}
	if consumeResp.Status != protocol.StatusOK {
		t.Fatalf("expected StatusOK, got %d", consumeResp.Status)
	}
	if len(consumeResp.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(consumeResp.Records))
	}
	if !bytes.Equal(consumeResp.Records[0].Value, rec1.Value) || !bytes.Equal(consumeResp.Records[1].Value, rec2.Value) {
		t.Fatalf("record value mismatch")
	}
}

func TestServer_OffsetCommitAndGet(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// 1. Commit offset
	commitReq := protocol.CommitReq{
		GroupID:     "indexer-group",
		Topic:       "orders",
		PartitionID: 1,
		Offset:      420,
		Metadata:    "node-a",
	}
	payload := protocol.EncodeCommitReq(commitReq)
	if err := protocol.WriteFrame(conn, protocol.OpCommitReq, payload); err != nil {
		t.Fatalf("write commit failed: %v", err)
	}

	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read commit resp failed: %v", err)
	}
	commitResp, err := protocol.DecodeCommitResp(frame.Payload)
	if err != nil {
		t.Fatalf("decode commit resp failed: %v", err)
	}
	if commitResp.Status != protocol.StatusOK {
		t.Fatalf("expected StatusOK, got %d", commitResp.Status)
	}

	// 2. Query offset
	offsetReq := protocol.OffsetReq{
		GroupID:     "indexer-group",
		Topic:       "orders",
		PartitionID: 1,
	}
	payload = protocol.EncodeOffsetReq(offsetReq)
	if err := protocol.WriteFrame(conn, protocol.OpOffsetReq, payload); err != nil {
		t.Fatalf("write offset req failed: %v", err)
	}

	frame, err = protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read offset resp failed: %v", err)
	}
	offsetResp, err := protocol.DecodeOffsetResp(frame.Payload)
	if err != nil {
		t.Fatalf("decode offset resp failed: %v", err)
	}
	if offsetResp.Status != protocol.StatusOK || offsetResp.Offset != 420 {
		t.Fatalf("unexpected offset resp: %+v", offsetResp)
	}
}

func TestServer_PartitionNotFound(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	consumeReq := protocol.ConsumeReq{
		Topic:       "nonexistent-topic",
		PartitionID: 99,
		Offset:      0,
		MaxBytes:    1024,
	}
	payload := protocol.EncodeConsumeReq(consumeReq)
	if err := protocol.WriteFrame(conn, protocol.OpConsumeReq, payload); err != nil {
		t.Fatalf("write consume failed: %v", err)
	}

	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read consume resp failed: %v", err)
	}
	consumeResp, err := protocol.DecodeConsumeResp(frame.Payload)
	if err != nil {
		t.Fatalf("decode consume resp failed: %v", err)
	}
	if consumeResp.Status != protocol.StatusErrPartitionNotFound {
		t.Fatalf("expected StatusErrPartitionNotFound, got %d", consumeResp.Status)
	}
}
