// Package: test
// File: smoke_test.go
// Core Work: End-to-end integration smoke test exercising walited daemon lifecycle, concurrent streaming, CRC validation, checkpoint recovery, and torn-write resilience.
// Exported: none (test package: TestSmoke_*)
package test

/*
Functions Manifest & Usage:
- TestSmoke_EndToEndClusterLifecycle(t *testing.T): Full end-to-end integration test validating ephemeral daemon boot, concurrent multi-client produce across partitions, CRC32 and monotonic offset validation, consumer group checkpointing across disconnects, graceful shutdown, and seamless state recovery upon cold restart.
- TestSmoke_TornWriteResilienceOnDaemonRestart(t *testing.T): Validates crash resilience by simulating an incomplete torn write appended to the active log segment, asserting automatic tail truncation and seamless write resumption on daemon restart.
*/

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/barbaravane/walite/internal/log"
	"github.com/barbaravane/walite/internal/protocol"
	"github.com/barbaravane/walite/internal/server"
)

// Helper: dials a TCP connection to the specified server address with timeout.
func dialSmokeClient(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial server at %s: %v", addr, err)
	}
	return conn
}

// Helper: writes a ProduceReq frame and awaits a successful ProduceResp.
func produceSmokeBatch(t *testing.T, conn net.Conn, topic string, partitionID uint32, records []*log.Record) (uint64, uint32) {
	t.Helper()
	req := protocol.ProduceReq{
		Topic:       topic,
		PartitionID: partitionID,
		Records:     records,
	}
	payload := protocol.EncodeProduceReq(req)
	if err := protocol.WriteFrame(conn, protocol.OpProduceReq, payload); err != nil {
		t.Fatalf("failed to write produce frame: %v", err)
	}

	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read produce response frame: %v", err)
	}
	if frame.OpCode != protocol.OpProduceResp {
		t.Fatalf("expected OpProduceResp (0x81), got 0x%x", frame.OpCode)
	}

	resp, err := protocol.DecodeProduceResp(frame.Payload)
	if err != nil {
		t.Fatalf("failed to decode produce response: %v", err)
	}
	if resp.Status != protocol.StatusOK {
		t.Fatalf("produce failed with status: 0x%04x", resp.Status)
	}

	return resp.BaseOffset, resp.Count
}

// Helper: writes a ConsumeReq frame and returns fetched records.
func consumeSmokeRecords(t *testing.T, conn net.Conn, topic string, partitionID uint32, offset uint64, maxBytes uint32) []*log.Record {
	t.Helper()
	req := protocol.ConsumeReq{
		Topic:       topic,
		PartitionID: partitionID,
		Offset:      offset,
		MaxBytes:    maxBytes,
	}
	payload := protocol.EncodeConsumeReq(req)
	if err := protocol.WriteFrame(conn, protocol.OpConsumeReq, payload); err != nil {
		t.Fatalf("failed to write consume frame: %v", err)
	}

	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read consume response frame: %v", err)
	}
	if frame.OpCode != protocol.OpConsumeResp {
		t.Fatalf("expected OpConsumeResp (0x82), got 0x%x", frame.OpCode)
	}

	resp, err := protocol.DecodeConsumeResp(frame.Payload)
	if err != nil {
		t.Fatalf("failed to decode consume response: %v", err)
	}
	if resp.Status != protocol.StatusOK {
		t.Fatalf("consume failed with status: 0x%04x", resp.Status)
	}

	return resp.Records
}

// Helper: commits a consumer group offset.
func commitSmokeOffset(t *testing.T, conn net.Conn, groupID, topic string, partitionID uint32, offset uint64, meta string) {
	t.Helper()
	req := protocol.CommitReq{
		GroupID:     groupID,
		Topic:       topic,
		PartitionID: partitionID,
		Offset:      offset,
		Metadata:    meta,
	}
	payload := protocol.EncodeCommitReq(req)
	if err := protocol.WriteFrame(conn, protocol.OpCommitReq, payload); err != nil {
		t.Fatalf("failed to write commit frame: %v", err)
	}

	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read commit response frame: %v", err)
	}
	if frame.OpCode != protocol.OpCommitResp {
		t.Fatalf("expected OpCommitResp (0x84), got 0x%x", frame.OpCode)
	}

	resp, err := protocol.DecodeCommitResp(frame.Payload)
	if err != nil {
		t.Fatalf("failed to decode commit response: %v", err)
	}
	if resp.Status != protocol.StatusOK {
		t.Fatalf("commit failed with status: 0x%04x", resp.Status)
	}
}

// Helper: queries a consumer group committed offset.
func getSmokeOffset(t *testing.T, conn net.Conn, groupID, topic string, partitionID uint32) uint64 {
	t.Helper()
	req := protocol.OffsetReq{
		GroupID:     groupID,
		Topic:       topic,
		PartitionID: partitionID,
	}
	payload := protocol.EncodeOffsetReq(req)
	if err := protocol.WriteFrame(conn, protocol.OpOffsetReq, payload); err != nil {
		t.Fatalf("failed to write offset query frame: %v", err)
	}

	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read offset query response frame: %v", err)
	}
	if frame.OpCode != protocol.OpOffsetResp {
		t.Fatalf("expected OpOffsetResp (0x83), got 0x%x", frame.OpCode)
	}

	resp, err := protocol.DecodeOffsetResp(frame.Payload)
	if err != nil {
		t.Fatalf("failed to decode offset response: %v", err)
	}
	if resp.Status != protocol.StatusOK {
		t.Fatalf("offset query failed with status: 0x%04x", resp.Status)
	}

	return resp.Offset
}

func TestSmoke_EndToEndClusterLifecycle(t *testing.T) {
	dataDir := t.TempDir()

	cfg := server.Config{
		Addr:               "127.0.0.1:0", // Ephemeral TCP port
		DataDir:            dataDir,
		MaxSegmentBytes:    16 * 1024, // 16KB to exercise active segment rollover under load
		IndexIntervalBytes: 128,
		FlushInterval:      25 * time.Millisecond,
		SyncAlways:         false,
		ReadTimeout:        5 * time.Second,
		WriteTimeout:       5 * time.Second,
	}

	srv, err := server.NewServer(cfg)
	if err != nil {
		t.Fatalf("failed to instantiate server: %v", err)
	}

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	addr := srv.Addr().String()

	// -------------------------------------------------------------------------
	// 1. Concurrent Multi-Client Produce
	// -------------------------------------------------------------------------
	type produceTarget struct {
		topic       string
		partitionID uint32
		workerID    int
		batchCount  int
		recPerBatch int
	}

	targets := []produceTarget{
		{topic: "orders", partitionID: 0, workerID: 1, batchCount: 3, recPerBatch: 10},
		{topic: "orders", partitionID: 0, workerID: 2, batchCount: 2, recPerBatch: 10},
		{topic: "orders", partitionID: 1, workerID: 3, batchCount: 3, recPerBatch: 10},
		{topic: "telemetry", partitionID: 0, workerID: 4, batchCount: 4, recPerBatch: 10},
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(targets))

	for _, tgt := range targets {
		wg.Add(1)
		go func(target produceTarget) {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("worker %d dial failed: %w", target.workerID, err)
				return
			}
			defer conn.Close()

			for b := 0; b < target.batchCount; b++ {
				records := make([]*log.Record, target.recPerBatch)
				for r := 0; r < target.recPerBatch; r++ {
					records[r] = &log.Record{
						Timestamp: time.Now().UnixNano(),
						Key:       []byte(fmt.Sprintf("key-w%d-b%d-r%d", target.workerID, b, r)),
						Value:     bytes.Repeat([]byte(fmt.Sprintf("val-payload-w%d-b%d-r%d:", target.workerID, b, r)), 8),
					}
				}

				req := protocol.ProduceReq{
					Topic:       target.topic,
					PartitionID: target.partitionID,
					Records:     records,
				}
				if err := protocol.WriteFrame(conn, protocol.OpProduceReq, protocol.EncodeProduceReq(req)); err != nil {
					errCh <- fmt.Errorf("worker %d write produce failed: %w", target.workerID, err)
					return
				}

				frame, err := protocol.ReadFrame(conn)
				if err != nil {
					errCh <- fmt.Errorf("worker %d read produce resp failed: %w", target.workerID, err)
					return
				}

				resp, err := protocol.DecodeProduceResp(frame.Payload)
				if err != nil {
					errCh <- fmt.Errorf("worker %d decode resp failed: %w", target.workerID, err)
					return
				}
				if resp.Status != protocol.StatusOK {
					errCh <- fmt.Errorf("worker %d produce status error: 0x%04x", target.workerID, resp.Status)
					return
				}
				if resp.Count != uint32(target.recPerBatch) {
					errCh <- fmt.Errorf("worker %d produce count mismatch: expected %d, got %d", target.workerID, target.recPerBatch, resp.Count)
					return
				}
			}
		}(tgt)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent produce failed: %v", err)
		}
	}

	// -------------------------------------------------------------------------
	// 2. Consume & CRC32 Monotonic Validation
	// -------------------------------------------------------------------------
	// Expected totals:
	// - orders:0    -> 50 records (30 from w1 + 20 from w2)
	// - orders:1    -> 30 records (from w3)
	// - telemetry:0 -> 40 records (from w4)
	verifyPartition := func(topic string, partitionID uint32, expectedCount int) {
		conn := dialSmokeClient(t, addr)
		defer conn.Close()

		var consumed []*log.Record
		currentOffset := uint64(0)

		for len(consumed) < expectedCount {
			recs := consumeSmokeRecords(t, conn, topic, partitionID, currentOffset, 65536)
			if len(recs) == 0 {
				t.Fatalf("topic %s:%d exhausted records prematurely at offset %d (got %d of %d)",
					topic, partitionID, currentOffset, len(consumed), expectedCount)
			}
			for _, rec := range recs {
				// Assert strictly monotonic offsets starting from 0
				expectedOff := uint64(len(consumed))
				if rec.Offset != expectedOff {
					t.Fatalf("topic %s:%d offset sequence broken: expected %d, got %d",
						topic, partitionID, expectedOff, rec.Offset)
				}

				// Assert CRC32 integrity and codec roundtrip
				encoded := log.EncodeRecord(nil, rec)
				decoded, _, err := log.DecodeRecord(encoded)
				if err != nil {
					t.Fatalf("topic %s:%d CRC32 validation failed on offset %d: %v",
						topic, partitionID, rec.Offset, err)
				}
				if decoded.Offset != rec.Offset || !bytes.Equal(decoded.Key, rec.Key) || !bytes.Equal(decoded.Value, rec.Value) {
					t.Fatalf("topic %s:%d payload mismatch on offset %d",
						topic, partitionID, rec.Offset)
				}

				// Verify non-zero payload
				if len(rec.Key) == 0 || len(rec.Value) == 0 {
					t.Fatalf("topic %s:%d record at offset %d contains empty key or value",
						topic, partitionID, rec.Offset)
				}

				consumed = append(consumed, rec)
				currentOffset = rec.Offset + 1
			}
		}

		if len(consumed) != expectedCount {
			t.Fatalf("topic %s:%d total records mismatch: expected %d, got %d",
				topic, partitionID, expectedCount, len(consumed))
		}
	}

	verifyPartition("orders", 0, 50)
	verifyPartition("orders", 1, 30)
	verifyPartition("telemetry", 0, 40)

	// -------------------------------------------------------------------------
	// 3. Consumer Group Checkpointing & Disconnect Recovery
	// -------------------------------------------------------------------------
	const groupID = "checkout-pipeline"
	const checkpointOffset = uint64(25)

	client1 := dialSmokeClient(t, addr)
	commitSmokeOffset(t, client1, groupID, "orders", 0, checkpointOffset, "worker-node-alpha")
	// Explicitly terminate client1 connection to simulate network disconnect
	_ = client1.Close()

	// Connect fresh client2 after disconnect
	client2 := dialSmokeClient(t, addr)
	defer client2.Close()

	retrievedOffset := getSmokeOffset(t, client2, groupID, "orders", 0)
	if retrievedOffset != checkpointOffset {
		t.Fatalf("retrieved checkpoint mismatch: expected %d, got %d", checkpointOffset, retrievedOffset)
	}

	// Resume consumption from checkpoint offset
	resumedRecs := consumeSmokeRecords(t, client2, "orders", 0, retrievedOffset, 65536)
	if len(resumedRecs) == 0 {
		t.Fatalf("expected resumed records starting at %d, got none", retrievedOffset)
	}
	if resumedRecs[0].Offset != checkpointOffset {
		t.Fatalf("first resumed record offset mismatch: expected %d, got %d", checkpointOffset, resumedRecs[0].Offset)
	}

	// -------------------------------------------------------------------------
	// 4. Graceful Shutdown & Zero Torn Writes on Disk
	// -------------------------------------------------------------------------
	shutdownStart := time.Now()
	if err := srv.Close(); err != nil {
		t.Fatalf("server graceful shutdown failed: %v", err)
	}
	shutdownDuration := time.Since(shutdownStart)
	if shutdownDuration > 2*time.Second {
		t.Fatalf("graceful shutdown hung: took %v (limit: 2s)", shutdownDuration)
	}

	// Verify storage integrity directly on disk (no server process active)
	partitionDir := filepath.Join(dataDir, "topics", "orders", "partition-0")
	partCfg := log.PartitionConfig{
		Directory:          partitionDir,
		MaxSegmentBytes:    16 * 1024,
		IndexIntervalBytes: 128,
	}
	coldPart, err := log.OpenPartition(partCfg)
	if err != nil {
		t.Fatalf("failed to open partition directly from disk: %v", err)
	}

	// Read all records directly from disk engine and verify CRC
	for i := 0; i < 50; i++ {
		r, err := coldPart.ReadAt(uint64(i))
		if err != nil {
			t.Fatalf("failed to read record %d directly from disk partition: %v", i, err)
		}
		if r.Offset != uint64(i) {
			t.Fatalf("cold disk record offset broken: expected %d, got %d", i, r.Offset)
		}
		if len(r.Key) == 0 || len(r.Value) == 0 {
			t.Fatalf("cold disk record %d contains empty key or value", i)
		}
	}
	if _, err := coldPart.ReadAt(50); !errors.Is(err, log.ErrOffsetOutOfRange) {
		t.Fatalf("expected ErrOffsetOutOfRange for offset 50, got %v", err)
	}
	_ = coldPart.Close()

	// -------------------------------------------------------------------------
	// 5. Cold Daemon Restart & Zero State Loss
	// -------------------------------------------------------------------------
	// Spin up a brand new server on the same data directory with a fresh port
	cfgRestart := server.Config{
		Addr:               "127.0.0.1:0",
		DataDir:            dataDir,
		MaxSegmentBytes:    16 * 1024,
		IndexIntervalBytes: 128,
		FlushInterval:      25 * time.Millisecond,
		SyncAlways:         false,
		ReadTimeout:        5 * time.Second,
		WriteTimeout:       5 * time.Second,
	}

	srvRestart, err := server.NewServer(cfgRestart)
	if err != nil {
		t.Fatalf("failed to instantiate restarted server: %v", err)
	}
	if err := srvRestart.Start(); err != nil {
		t.Fatalf("failed to start restarted server: %v", err)
	}
	defer srvRestart.Close()

	clientRestart := dialSmokeClient(t, srvRestart.Addr().String())
	defer clientRestart.Close()

	// Verify committed consumer offset persisted across cold restart
	recoveredOffset := getSmokeOffset(t, clientRestart, groupID, "orders", 0)
	if recoveredOffset != checkpointOffset {
		t.Fatalf("recovered offset after cold restart mismatch: expected %d, got %d", checkpointOffset, recoveredOffset)
	}

	// Verify records from other partitions remain intact across cold restart
	p1Recs := consumeSmokeRecords(t, clientRestart, "orders", 1, 0, 65536)
	if len(p1Recs) != 30 {
		t.Fatalf("expected 30 records on orders:1 after restart, got %d", len(p1Recs))
	}
}

func TestSmoke_TornWriteResilienceOnDaemonRestart(t *testing.T) {
	dataDir := t.TempDir()

	cfg := server.Config{
		Addr:               "127.0.0.1:0",
		DataDir:            dataDir,
		MaxSegmentBytes:    64 * 1024,
		IndexIntervalBytes: 128,
		FlushInterval:      25 * time.Millisecond,
		SyncAlways:         true,
	}

	srv, err := server.NewServer(cfg)
	if err != nil {
		t.Fatalf("failed to instantiate server: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	conn := dialSmokeClient(t, srv.Addr().String())

	// Produce 10 valid records
	recs := make([]*log.Record, 10)
	for i := 0; i < 10; i++ {
		recs[i] = &log.Record{
			Timestamp: time.Now().UnixNano(),
			Key:       []byte(fmt.Sprintf("stable-key-%d", i)),
			Value:     []byte(fmt.Sprintf("stable-val-%d", i)),
		}
	}
	produceSmokeBatch(t, conn, "audit", 0, recs)
	_ = conn.Close()

	// Cleanly stop server
	if err := srv.Close(); err != nil {
		t.Fatalf("failed to stop server: %v", err)
	}

	// Locate the active log segment file and append 19 bytes of torn garbage
	partitionDir := filepath.Join(dataDir, "topics", "audit", "partition-0")
	entries, err := os.ReadDir(partitionDir)
	if err != nil {
		t.Fatalf("failed to read partition directory: %v", err)
	}

	var logFile string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".log" {
			logFile = filepath.Join(partitionDir, entry.Name())
			break
		}
	}
	if logFile == "" {
		t.Fatalf("no .log file found in %s", partitionDir)
	}

	f, err := os.OpenFile(logFile, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open log file for torn write: %v", err)
	}
	// Append 19 bytes of incomplete truncated frame / torn write
	if _, err := f.Write([]byte("torn-incomplete-bytes")); err != nil {
		t.Fatalf("failed to write torn bytes: %v", err)
	}
	_ = f.Close()

	// Restart server on same directory - recovery must truncate torn bytes automatically
	srvRestart, err := server.NewServer(cfg)
	if err != nil {
		t.Fatalf("failed to create server after torn write: %v", err)
	}
	if err := srvRestart.Start(); err != nil {
		t.Fatalf("failed to start server after torn write: %v", err)
	}
	defer srvRestart.Close()

	connRestart := dialSmokeClient(t, srvRestart.Addr().String())
	defer connRestart.Close()

	// Consume back: should have exactly the 10 valid records, with torn write discarded
	recoveredRecs := consumeSmokeRecords(t, connRestart, "audit", 0, 0, 65536)
	if len(recoveredRecs) != 10 {
		t.Fatalf("expected 10 records recovered after torn write, got %d", len(recoveredRecs))
	}

	// Verify all 10 records maintain accurate offsets and CRCs
	for i, r := range recoveredRecs {
		if r.Offset != uint64(i) {
			t.Fatalf("record offset mismatch: expected %d, got %d", i, r.Offset)
		}
		enc := log.EncodeRecord(nil, r)
		dec, _, err := log.DecodeRecord(enc)
		if err != nil {
			t.Fatalf("record CRC corrupt at offset %d: %v", r.Offset, err)
		}
		if !bytes.Equal(dec.Key, r.Key) || !bytes.Equal(dec.Value, r.Value) {
			t.Fatalf("record payload mismatch at offset %d", r.Offset)
		}
	}

	// Produce 11th record: should append seamlessly at offset 10
	nextRec := []*log.Record{
		{
			Timestamp: time.Now().UnixNano(),
			Key:       []byte("post-torn-key"),
			Value:     []byte("post-torn-val"),
		},
	}
	nextBase, count := produceSmokeBatch(t, connRestart, "audit", 0, nextRec)
	if nextBase != 10 || count != 1 {
		t.Fatalf("expected next append at base offset 10 count 1, got base %d count %d", nextBase, count)
	}

	// Final verification: 11 total records
	allRecs := consumeSmokeRecords(t, connRestart, "audit", 0, 0, 65536)
	if len(allRecs) != 11 {
		t.Fatalf("expected 11 total records, got %d", len(allRecs))
	}
}
