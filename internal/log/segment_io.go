// Package: log
// File: segment_io.go
// Core Work: Record append serialization and index-accelerated binary search reads
// Exported: none (methods on Segment: Append, ReadAt)
package log

/*
Functions Manifest & Usage:
- (s *Segment) Append(rec *Record) (uint64, error): Appends a record to log, writes index entry if interval reached.
- (s *Segment) ReadAt(targetOffset uint64) (*Record, error): Reads a record at targetOffset using sparse index lookup and fast-scan.
*/

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
)

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
	if err != nil || n != len(s.appendBuf) {
		_ = s.logFile.Truncate(int64(pos))
		_, _ = s.logFile.Seek(int64(pos), io.SeekStart)
		if err != nil {
			return 0, fmt.Errorf("failed to write record to log: %w", err)
		}
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
