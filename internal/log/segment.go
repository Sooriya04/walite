// Package: log
// File: segment.go
// Core Work: Append-only log segment lifecycle, physical record writing, sparse indexing, and crash recovery
// Exported: SegmentConfig, Segment, OpenSegment, MaxSegmentBytes, ErrSegmentFull, ErrSegmentTooLarge, ErrOffsetPrecedesBase
package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
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

// SegmentConfig defines operational constraints for a single segment log & index pair.
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
	return SegmentConfig{
		BaseOffset:         s.nextOffset,
		MaxBytes:           maxBytes,
		IndexIntervalBytes: indexInterval,
		Directory:          filepath.Dir(s.logFile.Name()),
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
func (s *Segment) Append(rec *Record) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, os.ErrClosed
	}

	if rec.Offset != 0 && rec.Offset < s.baseOffset {
		return 0, ErrOffsetPrecedesBase
	}

	if s.nextOffset-s.baseOffset > math.MaxUint32 {
		return 0, ErrSegmentFull
	}

	if uint64(len(rec.Key))+uint64(len(rec.Value)) > MaxRecordPayload {
		return 0, ErrRecordTooLarge
	}

	rec.Offset = s.nextOffset
	s.appendBuf = EncodeRecord(s.appendBuf[:0], rec)

	encodedLen := uint64(len(s.appendBuf))
	if (s.maxBytes > 0 && (s.curBytes+encodedLen) > s.maxBytes) || (s.curBytes+encodedLen > MaxSegmentBytes) {
		return 0, ErrSegmentFull
	}

	pos := uint32(s.curBytes)
	relOffset := uint32(rec.Offset - s.baseOffset)

	n, err := s.logFile.Write(s.appendBuf)
	if err != nil {
		return 0, fmt.Errorf("failed to write record to log: %w", err)
	}
	if n != len(s.appendBuf) {
		return 0, io.ErrShortWrite
	}

	// Sparse indexing: write index entry only after log write succeeds
	if s.bytesSinceIdx >= s.indexInterval || s.curBytes == 0 {
		if err := s.index.WriteEntry(relOffset, pos); err != nil {
			_ = s.logFile.Truncate(int64(pos))
			_, _ = s.logFile.Seek(int64(pos), io.SeekStart)
			return 0, fmt.Errorf("failed to write index entry: %w", err)
		}
		s.bytesSinceIdx = 0
	}

	assignedOffset := s.nextOffset
	s.curBytes += encodedLen
	s.bytesSinceIdx += uint32(encodedLen)
	s.nextOffset++

	return assignedOffset, nil
}

// ReadAt reads a single record starting at the specified absolute offset.
// It snapshots the committed high-watermark boundaries, consults the sparse index,
// and streams record data using thread-safe pread syscalls without blocking concurrent appends.
func (s *Segment) ReadAt(targetOffset uint64) (*Record, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, os.ErrClosed
	}
	baseOffset := s.baseOffset
	nextOffset := s.nextOffset
	maxBytes := s.curBytes
	s.mu.RUnlock()

	if targetOffset < baseOffset {
		return nil, ErrOffsetPrecedesBase
	}
	if targetOffset >= nextOffset {
		return nil, io.EOF
	}

	relOffset := uint32(targetOffset - baseOffset)
	startPos := uint32(0)

	// Consult sparse index for nearest indexed byte position
	if entry, err := s.index.Lookup(relOffset); err == nil {
		startPos = entry.Position
	}

	curPos := int64(startPos)
	headerBuf := make([]byte, HeaderSize)

	for curPos < int64(maxBytes) {
		n, err := s.logFile.ReadAt(headerBuf, curPos)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read record header at pos %d: %w", curPos, err)
		}
		if n < HeaderSize {
			return nil, io.ErrUnexpectedEOF
		}

		keyLen, valLen, err := ParseHeaderLengths(headerBuf)
		if err != nil {
			return nil, err
		}

		totalRecordLen := int64(HeaderSize + keyLen + valLen)
		if curPos+totalRecordLen > int64(maxBytes) {
			return nil, io.EOF
		}

		recOffset := binary.BigEndian.Uint64(headerBuf[0:8])
		if recOffset < targetOffset {
			// Fast-skip intermediary records: advance offset without payload disk reads or memory allocations
			curPos += totalRecordLen
			continue
		}
		if recOffset > targetOffset {
			return nil, io.EOF
		}

		fullBuf := make([]byte, totalRecordLen)
		copy(fullBuf[:HeaderSize], headerBuf)

		if totalRecordLen > HeaderSize {
			n, err = s.logFile.ReadAt(fullBuf[HeaderSize:], curPos+HeaderSize)
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("failed to read payload at pos %d: %w", curPos+HeaderSize, err)
			}
			if int64(n) < totalRecordLen-HeaderSize {
				return nil, io.ErrUnexpectedEOF
			}
		}

		rec, _, err := DecodeRecord(fullBuf)
		if err != nil {
			return nil, err
		}

		return rec, nil
	}

	return nil, io.EOF
}

// recoverAndSync scans the log file from the beginning, validating record integrity,
// truncating any trailing corrupt or torn writes, and synchronizing nextOffset and index state.
func (s *Segment) recoverAndSync() error {
	stat, err := s.logFile.Stat()
	if err != nil {
		return err
	}

	fileSize := stat.Size()
	headerBuf := make([]byte, HeaderSize)
	var validBytes int64
	var lastOffset uint64 = s.baseOffset
	var hasRecords bool

	for validBytes < fileSize {
		remaining := fileSize - validBytes
		if remaining < int64(HeaderSize) {
			// Torn write in header; truncate
			break
		}

		n, err := s.logFile.ReadAt(headerBuf, validBytes)
		if err != nil && err != io.EOF {
			return fmt.Errorf("recovery read header failed at %d: %w", validBytes, err)
		}
		if n < HeaderSize {
			break
		}

		keyLen, valLen, err := ParseHeaderLengths(headerBuf)
		if err != nil {
			break
		}

		totalRecordLen := int64(HeaderSize + keyLen + valLen)
		if remaining < totalRecordLen {
			// Partial record write; truncate
			break
		}

		fullBuf := make([]byte, totalRecordLen)
		_, err = s.logFile.ReadAt(fullBuf, validBytes)
		if err != nil && err != io.EOF {
			return fmt.Errorf("recovery read payload failed at %d: %w", validBytes, err)
		}

		rec, _, err := DecodeRecord(fullBuf)
		if err != nil {
			// Corrupt CRC32 checksum; truncate from here
			break
		}

		// Enforce strict monotonicity and continuity of offsets during recovery
		if !hasRecords {
			if rec.Offset != s.baseOffset {
				// First record does not match baseOffset; stale/recycled disk sector
				break
			}
		} else {
			if rec.Offset != lastOffset+1 {
				// Offset jump or regression detected; truncate from here
				break
			}
		}

		lastOffset = rec.Offset
		hasRecords = true
		validBytes += totalRecordLen
	}

	// Truncate trailing torn bytes if necessary
	if validBytes < fileSize {
		if err := s.logFile.Truncate(validBytes); err != nil {
			return fmt.Errorf("failed to truncate corrupt log trailing bytes: %w", err)
		}
	}

	s.curBytes = uint64(validBytes)
	if hasRecords {
		s.nextOffset = lastOffset + 1
	} else {
		s.nextOffset = s.baseOffset
	}

	// Rebuild or align index entries with validBytes
	idxCount := s.index.EntriesCount()
	validIdxCount := uint32(0)
	for i := uint32(0); i < idxCount; i++ {
		entry, err := s.index.ReadAt(i)
		if err != nil {
			break
		}
		if uint64(entry.Position) < s.curBytes {
			validIdxCount++
		} else {
			break
		}
	}
	if validIdxCount < idxCount {
		if err := s.index.Truncate(validIdxCount); err != nil {
			return fmt.Errorf("failed to align index with truncated log: %w", err)
		}
	}

	if validIdxCount == 0 && hasRecords {
		// Index is empty or was wiped; rebuild index entries from valid log records
		var scanBytes int64
		var bytesSince uint32
		for scanBytes < validBytes {
			pos := uint32(scanBytes)
			n, err := s.logFile.ReadAt(headerBuf, scanBytes)
			if err != nil || n < HeaderSize {
				return fmt.Errorf("failed reading header during index rebuild at %d: %w", scanBytes, err)
			}
			keyLen, valLen, _ := ParseHeaderLengths(headerBuf)
			recordOffset := binary.BigEndian.Uint64(headerBuf[0:8])
			totalRecordLen := int64(HeaderSize + keyLen + valLen)

			if bytesSince >= s.indexInterval || scanBytes == 0 {
				relOffset := uint32(recordOffset - s.baseOffset)
				if err := s.index.WriteEntry(relOffset, pos); err != nil {
					return fmt.Errorf("failed to write rebuilt index entry: %w", err)
				}
				bytesSince = 0
			}
			bytesSince += uint32(totalRecordLen)
			scanBytes += totalRecordLen
		}
		s.bytesSinceIdx = bytesSince
	} else if validIdxCount > 0 {
		lastEntry, err := s.index.ReadAt(validIdxCount - 1)
		if err == nil {
			unindexedBytes := uint32(s.curBytes - uint64(lastEntry.Position))
			if unindexedBytes >= s.indexInterval {
				// Index lagged behind log during crash; scan from lastEntry.Position to backfill missing entries
				scanBytes := int64(lastEntry.Position)
				bytesSince := uint32(0)
				for scanBytes < validBytes {
					pos := uint32(scanBytes)
					n, err := s.logFile.ReadAt(headerBuf, scanBytes)
					if err != nil || n < HeaderSize {
						return fmt.Errorf("failed reading header during lagged index rebuild at %d: %w", scanBytes, err)
					}
					keyLen, valLen, _ := ParseHeaderLengths(headerBuf)
					recordOffset := binary.BigEndian.Uint64(headerBuf[0:8])
					totalRecordLen := int64(HeaderSize + keyLen + valLen)

					if pos > lastEntry.Position && bytesSince >= s.indexInterval {
						relOffset := uint32(recordOffset - s.baseOffset)
						if err := s.index.WriteEntry(relOffset, pos); err != nil {
							return fmt.Errorf("failed to write lagged index entry: %w", err)
						}
						bytesSince = 0
					}
					bytesSince += uint32(totalRecordLen)
					scanBytes += totalRecordLen
				}
				s.bytesSinceIdx = bytesSince
			} else {
				s.bytesSinceIdx = unindexedBytes
			}
		}
	} else {
		s.bytesSinceIdx = 0
	}

	return nil
}

// Sync flushes data and index writes to persistent storage.
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}

	if err := s.logFile.Sync(); err != nil {
		return err
	}
	return s.index.Sync()
}

// Close syncs and closes both the log and index files.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true

	var firstErr error
	if err := s.logFile.Sync(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := s.logFile.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := s.index.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

// IndexEntriesCount returns the number of sparse index entries in the segment.
func (s *Segment) IndexEntriesCount() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.EntriesCount()
}
