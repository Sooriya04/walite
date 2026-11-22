// Package: log
// File: segment_recovery.go
// Core Work: Crash recovery, torn write truncation, and sparse index rebuilding
// Exported: none (internal method on Segment: recoverAndSync)
package log

/*
Functions Manifest & Usage:
- (s *Segment) recoverAndSync() error: Scans data file, truncates torn/corrupt writes, aligns nextOffset, and rebuilds missing/lagged index.
*/

import (
	"encoding/binary"
	"fmt"
	"io"
)

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
