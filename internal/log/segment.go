// Package: log
// File: segment.go
// Core Work: Segment structure definition, lifecycle bootstrap, and metadata inspection
// Exported: SegmentConfig, Segment, FormatSegmentFileName, OpenSegment, MaxSegmentBytes, ErrSegmentFull, ErrSegmentTooLarge, ErrOffsetPrecedesBase
package log

/*
Functions Manifest & Usage:
- FormatSegmentFileName(baseOffset uint64, ext string) string: Formats 20-digit zero-padded segment filenames.
- OpenSegment(cfg SegmentConfig) (*Segment, error): Opens/creates a segment, initializes index, and recovers torn writes.
- (s *Segment) BaseOffset() uint64: Returns the starting offset for this segment.
- (s *Segment) NextSegmentConfig(maxBytes uint64, indexInterval uint32) SegmentConfig: Derives next segment configuration.
- (s *Segment) NextOffset() uint64: Returns the next monotonic record offset to be appended.
- (s *Segment) CurrentSize() uint64: Returns current physical byte size of the segment data file.
- (s *Segment) IsFull() bool: Checks whether the segment has reached MaxBytes capacity limit.
*/

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var (
	// ErrSegmentFull is returned when appending a record would exceed the configured MaxBytes limit.
	ErrSegmentFull = errors.New("segment is full: write exceeds maximum segment bytes")
	// ErrOffsetPrecedesBase is returned when appending a record whose absolute offset is smaller than baseOffset.
	ErrOffsetPrecedesBase = errors.New("record offset precedes segment base offset")
	// ErrSegmentTooLarge is returned when SegmentConfig.MaxBytes exceeds MaxSegmentBytes.
	ErrSegmentTooLarge = errors.New("walite: configured max segment bytes exceeds 4GB index boundary")
)

// MaxSegmentBytes defines the maximum physical size of a segment file (4GB - 1).
// It prevents 32-bit integer overflow in the sparse index position field.
const MaxSegmentBytes uint64 = 1<<32 - 1

type SegmentConfig struct {
	BaseOffset         uint64
	MaxBytes           uint64
	IndexIntervalBytes uint32
	Directory          string
}

// Segment represents an active or historical partition segment comprising a `.log` data file
// and a paired `.index` binary sparse lookup file.
type Segment struct {
	mu            sync.RWMutex
	dir           string
	baseOffset    uint64
	nextOffset    uint64
	maxBytes      uint64
	indexInterval uint32
	bytesSinceIdx uint32
	curBytes      uint64

	logFile   *os.File
	index     *Index
	closed    bool
	appendBuf []byte
}

// FormatSegmentFileName formats a 64-bit base offset into a 20-digit zero-padded filename with the given extension.
func FormatSegmentFileName(baseOffset uint64, ext string) string {
	return fmt.Sprintf("%020d%s", baseOffset, ext)
}

// OpenSegment opens an existing segment or creates a new segment file pair (`.log` and `.index`)
// at baseOffset inside cfg.Directory. It validates integrity and recovers from torn writes.
func OpenSegment(cfg SegmentConfig) (*Segment, error) {
	if cfg.MaxBytes > MaxSegmentBytes {
		return nil, ErrSegmentTooLarge
	}

	if cfg.IndexIntervalBytes == 0 {
		cfg.IndexIntervalBytes = DefaultIndexIntervalBytes
	}

	if err := os.MkdirAll(cfg.Directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create segment directory: %w", err)
	}

	logPath := filepath.Join(cfg.Directory, FormatSegmentFileName(cfg.BaseOffset, ".log"))
	indexPath := filepath.Join(cfg.Directory, FormatSegmentFileName(cfg.BaseOffset, ".index"))

	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment log file: %w", err)
	}

	index, err := OpenIndex(indexPath)
	if err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("failed to open segment index file: %w", err)
	}

	seg := &Segment{
		dir:           cfg.Directory,
		baseOffset:    cfg.BaseOffset,
		nextOffset:    cfg.BaseOffset,
		maxBytes:      cfg.MaxBytes,
		indexInterval: cfg.IndexIntervalBytes,
		logFile:       logFile,
		index:         index,
	}

	if err := seg.recoverAndSync(); err != nil {
		_ = seg.Close()
		return nil, fmt.Errorf("failed to recover segment: %w", err)
	}

	return seg, nil
}

// BaseOffset returns the lowest absolute offset represented by this segment.
func (s *Segment) BaseOffset() uint64 {
	return s.baseOffset
}

// NextSegmentConfig generates a SegmentConfig for the subsequent contiguous segment.
func (s *Segment) NextSegmentConfig(maxBytes uint64, indexInterval uint32) SegmentConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dir := s.dir
	if dir == "" && s.logFile != nil {
		dir = filepath.Dir(s.logFile.Name())
	}
	return SegmentConfig{
		BaseOffset:         s.nextOffset,
		MaxBytes:           maxBytes,
		IndexIntervalBytes: indexInterval,
		Directory:          dir,
	}
}

// NextOffset returns the next absolute offset that will be assigned to a new record.
func (s *Segment) NextOffset() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextOffset
}

// CurrentSize returns the physical byte size of the segment log file.
func (s *Segment) CurrentSize() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.curBytes
}

// IsFull returns true if appending additional data exceeds maxBytes.
func (s *Segment) IsFull() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.maxBytes == 0 {
		return false
	}
	return s.curBytes >= s.maxBytes
}

// Append appends a record payload to the segment log. If record.Offset does not match nextOffset,
// it is stamped with nextOffset. If adding the record breaches maxBytes, ErrSegmentFull is returned.
