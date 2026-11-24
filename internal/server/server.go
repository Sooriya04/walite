// Package: server
// File: server.go
// Core Work: High-throughput TCP server, connection pooling, request routing, and graceful shutdown.
// Exported: Server, NewServer, ErrServerClosed
package server

/*
Functions Manifest & Usage:
- NewServer(cfg Config) (*Server, error): Bootstraps a new walited server with partition registry and offset manager.
- (s *Server) Start() error: Binds TCP listener and launches the concurrent connection accept loop.
- (s *Server) Addr() net.Addr: Returns the bound network address of the listener.
- (s *Server) Close() error: Gracefully halts connection acceptance, flushes partitions to disk, and closes resources.
- (s *Server) acceptLoop(): Continuously accepts inbound client TCP connections until shutdown.
- (s *Server) handleConn(conn net.Conn): Handles stream protocol framing for a single client connection.
- (s *Server) getOrCreatePartition(topic string, partitionID uint32) (*log.Partition, error): Thread-safely fetches or creates a partition.
- (s *Server) getPartition(topic string, partitionID uint32) *log.Partition: Thread-safely reads existing partition without creating.
- (s *Server) writeResponse(conn net.Conn, opCode uint8, payload []byte) error: Sets write deadline and writes framed response.
- (s *Server) handleProduce(conn net.Conn, payload []byte) error: Appends batch of records to specified partition.
- (s *Server) handleConsume(conn net.Conn, payload []byte) error: Reads sequential records starting at offset up to max bytes.
- (s *Server) handleOffsetGet(conn net.Conn, payload []byte) error: Queries consumer group checkpoint offset.
- (s *Server) handleOffsetCommit(conn net.Conn, payload []byte) error: Commits consumer group checkpoint.
- (s *Server) handlePing(conn net.Conn, payload []byte) error: Echoes ping heartbeat back as pong response.
*/

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/barbaravane/walite/internal/consumer"
	"github.com/barbaravane/walite/internal/log"
	"github.com/barbaravane/walite/internal/protocol"
)

// ErrServerClosed is returned when an operation is attempted on a stopped server.
var ErrServerClosed = errors.New("walite: server closed")

// Server encapsulates the walited network engine, managing partitions and client connections.
type Server struct {
	cfg        Config
	listener   net.Listener
	mu         sync.RWMutex
	partitions map[string]map[uint32]*log.Partition
	offsetMgr  *consumer.OffsetManager
	conns      map[net.Conn]struct{}
	closed     bool
	wg         sync.WaitGroup
	quit       chan struct{}
}

// NewServer initializes a Server instance, creating required storage paths and opening the offset manager.
func NewServer(cfg Config) (*Server, error) {
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create server data directory: %w", err)
	}

	omDir := filepath.Join(cfg.DataDir, "__consumer_offsets")
	omCfg := consumer.OffsetManagerConfig{
		Directory:       omDir,
		SyncOnCommit:    cfg.SyncAlways,
		MaxSegmentBytes: cfg.MaxSegmentBytes,
	}
	om, err := consumer.OpenOffsetManager(omCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open offset manager: %w", err)
	}

	return &Server{
		cfg:        cfg,
		partitions: make(map[string]map[uint32]*log.Partition),
		offsetMgr:  om,
		conns:      make(map[net.Conn]struct{}),
		quit:       make(chan struct{}),
	}, nil
}

// Start binds to the configured TCP address and starts accepting incoming client connections.
func (s *Server) Start() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrServerClosed
	}

	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.listener = ln
	s.mu.Unlock()

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Addr returns the listening network address.
func (s *Server) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Close gracefully terminates the server, halting listeners, closing client sockets, draining workers, and syncing storage.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.quit)

	var closeErr error
	if s.listener != nil {
		closeErr = s.listener.Close()
	}

	// Immediately close all active client network connections.
	// This unblocks any blocked protocol.ReadFrame / io.ReadFull calls with net.ErrClosed,
	// allowing all handleConn goroutines to cleanly exit without hanging.
	for conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()

	// Wait for acceptLoop and all connection goroutines to fully exit.
	// Since all sockets are closed and s.closed prevents any new s.wg.Add,
	// all goroutines terminate within sub-milliseconds without WaitGroup races.
	s.wg.Wait()

	// Now that all client goroutines have completely exited, safely flush and close partitions and offset manager.
	s.mu.Lock()
	for _, parts := range s.partitions {
		for _, part := range parts {
			_ = part.Sync()
			_ = part.Close()
		}
	}

	if s.offsetMgr != nil {
		_ = s.offsetMgr.Sync()
		_ = s.offsetMgr.Close()
	}
	s.mu.Unlock()

	return closeErr
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.RLock()
			closed := s.closed
			s.mu.RUnlock()
			if closed {
				return
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}

		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()

		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.wg.Done()
	}()

	for {
		select {
		case <-s.quit:
			return
		default:
		}

		if s.cfg.ReadTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout)); err != nil {
				return
			}
		}

		frame, err := protocol.ReadFrame(conn)
		if err != nil {
			return
		}

		var handleErr error
		switch frame.OpCode {
		case protocol.OpProduceReq:
			handleErr = s.handleProduce(conn, frame.Payload)
		case protocol.OpConsumeReq:
			handleErr = s.handleConsume(conn, frame.Payload)
		case protocol.OpOffsetReq:
			handleErr = s.handleOffsetGet(conn, frame.Payload)
		case protocol.OpCommitReq:
			handleErr = s.handleOffsetCommit(conn, frame.Payload)
		case protocol.OpPingReq:
			handleErr = s.handlePing(conn, frame.Payload)
		default:
			handleErr = s.writeResponse(conn, protocol.OpProduceResp, protocol.EncodeProduceResp(protocol.ProduceResp{
				Status: protocol.StatusErrUnknown,
			}))
		}

		if handleErr != nil {
			return
		}
	}
}

func (s *Server) getOrCreatePartition(topic string, partitionID uint32) (*log.Partition, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrServerClosed
	}
	if parts, ok := s.partitions[topic]; ok {
		if part, exists := parts[partitionID]; exists {
			s.mu.RUnlock()
			return part, nil
		}
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrServerClosed
	}

	if parts, ok := s.partitions[topic]; ok {
		if part, exists := parts[partitionID]; exists {
			return part, nil
		}
	} else {
		s.partitions[topic] = make(map[uint32]*log.Partition)
	}

	partDir := filepath.Join(s.cfg.DataDir, "topics", topic, fmt.Sprintf("partition-%d", partitionID))
	partCfg := log.PartitionConfig{
		Directory:          partDir,
		MaxSegmentBytes:    s.cfg.MaxSegmentBytes,
		IndexIntervalBytes: s.cfg.IndexIntervalBytes,
		Flush: log.FlusherConfig{
			Interval: s.cfg.FlushInterval,
		},
	}

	part, err := log.OpenPartition(partCfg)
	if err != nil {
		return nil, err
	}

	s.partitions[topic][partitionID] = part
	return part, nil
}

func (s *Server) getPartition(topic string, partitionID uint32) *log.Partition {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil
	}
	if parts, ok := s.partitions[topic]; ok {
		return parts[partitionID]
	}
	return nil
}


func (s *Server) writeResponse(conn net.Conn, opCode uint8, payload []byte) error {
	if s.cfg.WriteTimeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout)); err != nil {
			return err
		}
	}
	return protocol.WriteFrame(conn, opCode, payload)
}

func (s *Server) handleProduce(conn net.Conn, payload []byte) error {
	req, err := protocol.DecodeProduceReq(payload)
	if err != nil {
		resp := protocol.EncodeProduceResp(protocol.ProduceResp{
			Status: protocol.StatusErrInvalidPayload,
		})
		return s.writeResponse(conn, protocol.OpProduceResp, resp)
	}

	part, err := s.getOrCreatePartition(req.Topic, req.PartitionID)
	if err != nil {
		resp := protocol.EncodeProduceResp(protocol.ProduceResp{
			Status: protocol.StatusErrUnknown,
		})
		return s.writeResponse(conn, protocol.OpProduceResp, resp)
	}

	if len(req.Records) == 0 {
		resp := protocol.EncodeProduceResp(protocol.ProduceResp{
			Status:     protocol.StatusOK,
			BaseOffset: part.NextOffset(),
			Count:      0,
		})
		return s.writeResponse(conn, protocol.OpProduceResp, resp)
	}

	var baseOffset uint64
	var count uint32

	for i, rec := range req.Records {
		off, appendErr := part.Append(rec)
		if appendErr != nil {
			resp := protocol.EncodeProduceResp(protocol.ProduceResp{
				Status:     protocol.StatusErrUnknown,
				BaseOffset: baseOffset,
				Count:      count,
			})
			return s.writeResponse(conn, protocol.OpProduceResp, resp)
		}
		if i == 0 {
			baseOffset = off
		}
		count++
	}

	if s.cfg.SyncAlways {
		_ = part.Sync()
	}

	resp := protocol.EncodeProduceResp(protocol.ProduceResp{
		Status:     protocol.StatusOK,
		BaseOffset: baseOffset,
		Count:      count,
	})
	return s.writeResponse(conn, protocol.OpProduceResp, resp)
}

func (s *Server) handleConsume(conn net.Conn, payload []byte) error {
	req, err := protocol.DecodeConsumeReq(payload)
	if err != nil {
		resp := protocol.EncodeConsumeResp(protocol.ConsumeResp{
			Status: protocol.StatusErrInvalidPayload,
		})
		return s.writeResponse(conn, protocol.OpConsumeResp, resp)
	}

	part := s.getPartition(req.Topic, req.PartitionID)
	if part == nil {
		resp := protocol.EncodeConsumeResp(protocol.ConsumeResp{
			Status: protocol.StatusErrPartitionNotFound,
		})
		return s.writeResponse(conn, protocol.OpConsumeResp, resp)
	}

	nextOffset := part.NextOffset()
	oldestOffset := part.OldestOffset()

	if req.Offset < oldestOffset || req.Offset > nextOffset {
		resp := protocol.EncodeConsumeResp(protocol.ConsumeResp{
			Status: protocol.StatusErrOffsetOutOfRange,
		})
		return s.writeResponse(conn, protocol.OpConsumeResp, resp)
	}

	if req.Offset == nextOffset {
		// Client is at the active head of partition; return empty records with StatusOK
		resp := protocol.EncodeConsumeResp(protocol.ConsumeResp{
			Status:  protocol.StatusOK,
			Records: nil,
		})
		return s.writeResponse(conn, protocol.OpConsumeResp, resp)
	}

	var records []*log.Record
	var accumulatedBytes uint32
	currentOffset := req.Offset

	for currentOffset < nextOffset {
		rec, readErr := part.ReadAt(currentOffset)
		if readErr != nil {
			if errors.Is(readErr, log.ErrOffsetOutOfRange) || errors.Is(readErr, io.EOF) {
				break
			}
			break
		}

		recBytes := uint32(log.HeaderSize + len(rec.Key) + len(rec.Value))
		if len(records) > 0 && accumulatedBytes+recBytes > req.MaxBytes {
			break
		}

		records = append(records, rec)
		accumulatedBytes += recBytes
		currentOffset++

		if accumulatedBytes >= req.MaxBytes {
			break
		}
	}

	resp := protocol.EncodeConsumeResp(protocol.ConsumeResp{
		Status:  protocol.StatusOK,
		Records: records,
	})
	return s.writeResponse(conn, protocol.OpConsumeResp, resp)
}

func (s *Server) handleOffsetGet(conn net.Conn, payload []byte) error {
	req, err := protocol.DecodeOffsetReq(payload)
	if err != nil {
		resp := protocol.EncodeOffsetResp(protocol.OffsetResp{
			Status: protocol.StatusErrInvalidPayload,
		})
		return s.writeResponse(conn, protocol.OpOffsetResp, resp)
	}

	offset, getErr := s.offsetMgr.GetOffset(req.GroupID, req.Topic, req.PartitionID)
	if getErr != nil {
		if errors.Is(getErr, consumer.ErrOffsetNotFound) {
			resp := protocol.EncodeOffsetResp(protocol.OffsetResp{
				Status: protocol.StatusErrOffsetOutOfRange,
				Offset: 0,
			})
			return s.writeResponse(conn, protocol.OpOffsetResp, resp)
		}
		resp := protocol.EncodeOffsetResp(protocol.OffsetResp{
			Status: protocol.StatusErrUnknown,
		})
		return s.writeResponse(conn, protocol.OpOffsetResp, resp)
	}

	resp := protocol.EncodeOffsetResp(protocol.OffsetResp{
		Status: protocol.StatusOK,
		Offset: offset,
	})
	return s.writeResponse(conn, protocol.OpOffsetResp, resp)
}

func (s *Server) handleOffsetCommit(conn net.Conn, payload []byte) error {
	req, err := protocol.DecodeCommitReq(payload)
	if err != nil {
		resp := protocol.EncodeCommitResp(protocol.CommitResp{
			Status: protocol.StatusErrInvalidPayload,
		})
		return s.writeResponse(conn, protocol.OpCommitResp, resp)
	}

	cp := consumer.Checkpoint{
		GroupID:     req.GroupID,
		Topic:       req.Topic,
		PartitionID: req.PartitionID,
		Offset:      req.Offset,
		Timestamp:   time.Now().UnixNano(),
		Metadata:    req.Metadata,
	}

	if commitErr := s.offsetMgr.CommitOffset(cp); commitErr != nil {
		resp := protocol.EncodeCommitResp(protocol.CommitResp{
			Status: protocol.StatusErrUnknown,
		})
		return s.writeResponse(conn, protocol.OpCommitResp, resp)
	}

	resp := protocol.EncodeCommitResp(protocol.CommitResp{
		Status: protocol.StatusOK,
	})
	return s.writeResponse(conn, protocol.OpCommitResp, resp)
}

func (s *Server) handlePing(conn net.Conn, payload []byte) error {
	req, err := protocol.DecodePingReq(payload)
	if err != nil {
		return s.writeResponse(conn, protocol.OpPongResp, nil)
	}

	resp := protocol.EncodePongResp(protocol.PongResp{
		Echo: req.Echo,
	})
	return s.writeResponse(conn, protocol.OpPongResp, resp)
}
