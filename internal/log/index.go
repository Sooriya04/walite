// Package: log
// File: index.go
// Core Work: Fixed-size binary sparse indexing with binary search lookups for log segments
// Exported: IndexEntry, Index, OpenIndex, ErrIndexCorrupt
package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	// EntrySize is the fixed size in bytes of each index entry.
	// 4 bytes RelativeOffset + 4 bytes Physical Position = 8 bytes.
	EntrySize = 8

	// DefaultIndexIntervalBytes is the default byte distance between index writes.
	DefaultIndexIntervalBytes uint32 = 4096
)

var (
	// ErrIndexCorrupt indicates that the index file size is not an exact multiple of EntrySize.
	ErrIndexCorrupt = errors.New("index file is corrupted: invalid size")
	// ErrIndexOutOfBounds is returned when querying an empty index or invalid target offset.
	ErrIndexOutOfBounds = errors.New("target offset precedes first indexed entry")
)

// IndexEntry represents a single mapping between a segment-relative record offset
// and its exact physical byte position in the corresponding segment log file.
type IndexEntry struct {
	RelativeOffset uint32
	Position       uint32
}

// Index provides thread-safe append and binary-search lookup operations over
// an append-only, fixed-size entry index file on disk.
type Index struct {
	mu   sync.RWMutex
	file *os.File
	size uint32 // count of entries in the index
}

// OpenIndex opens or creates an index file at the specified path.
// It validates that existing files have sizes aligned to 8-byte boundaries.
func OpenIndex(path string) (*Index, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to stat index file: %w", err)
	}

	fileSize := stat.Size()
	if fileSize%EntrySize != 0 {
		_ = file.Close()
		return nil, ErrIndexCorrupt
	}

	return &Index{
		file: file,
		size: uint32(fileSize / EntrySize),
	}, nil
}

// EntriesCount returns the total number of entries currently stored in the index.
func (idx *Index) EntriesCount() uint32 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.size
}

// WriteEntry appends a new (relativeOffset, position) entry to the index file.
// It verifies that relativeOffset is monotonically non-decreasing.
func (idx *Index) WriteEntry(relativeOffset, position uint32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	var buf [EntrySize]byte
	binary.BigEndian.PutUint32(buf[0:4], relativeOffset)
	binary.BigEndian.PutUint32(buf[4:8], position)

	n, err := idx.file.Write(buf[:])
	if err != nil {
		return fmt.Errorf("failed to write index entry: %w", err)
	}
	if n != EntrySize {
		return io.ErrShortWrite
	}

	idx.size++
	return nil
}

// ReadAt reads the index entry at the given entry index (0-indexed).
func (idx *Index) ReadAt(entryIdx uint32) (IndexEntry, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.readAtLocked(entryIdx)
}

func (idx *Index) readAtLocked(entryIdx uint32) (IndexEntry, error) {
	if entryIdx >= idx.size {
		return IndexEntry{}, io.EOF
	}

	var buf [EntrySize]byte
	offset := int64(entryIdx) * EntrySize
	n, err := idx.file.ReadAt(buf[:], offset)
	if err != nil && err != io.EOF {
		return IndexEntry{}, fmt.Errorf("failed to read index entry at %d: %w", entryIdx, err)
	}
	if n < EntrySize {
		return IndexEntry{}, io.ErrUnexpectedEOF
	}

	return IndexEntry{
		RelativeOffset: binary.BigEndian.Uint32(buf[0:4]),
		Position:       binary.BigEndian.Uint32(buf[4:8]),
	}, nil
}

// Lookup performs a binary search over the sorted index to locate the highest
// index entry whose RelativeOffset is <= targetRelOffset.
// This allows the segment scanner to jump directly to the closest predecessor position on disk.
func (idx *Index) Lookup(targetRelOffset uint32) (IndexEntry, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.size == 0 {
		return IndexEntry{}, ErrIndexOutOfBounds
	}

	firstEntry, err := idx.readAtLocked(0)
	if err != nil {
		return IndexEntry{}, err
	}
	if targetRelOffset < firstEntry.RelativeOffset {
		return IndexEntry{}, ErrIndexOutOfBounds
	}

	low := uint32(0)
	high := idx.size - 1
	bestIdx := uint32(0)

	for low <= high {
		mid := low + (high-low)/2
		entry, err := idx.readAtLocked(mid)
		if err != nil {
			return IndexEntry{}, err
		}

		if entry.RelativeOffset <= targetRelOffset {
			bestIdx = mid
			if mid == ^uint32(0) { // prevent overflow check
				break
			}
			low = mid + 1
		} else {
			if mid == 0 {
				break
			}
			high = mid - 1
		}
	}

	return idx.readAtLocked(bestIdx)
}

// Truncate resizes the index to contain exactly count entries, truncating physical file size.
func (idx *Index) Truncate(count uint32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	newBytes := int64(count) * EntrySize
	if err := idx.file.Truncate(newBytes); err != nil {
		return fmt.Errorf("failed to truncate index file: %w", err)
	}

	if _, err := idx.file.Seek(newBytes, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek index after truncate: %w", err)
	}

	idx.size = count
	return nil
}

// Sync flushes file changes to disk.
func (idx *Index) Sync() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.file.Sync()
}

// Close flushes and closes the backing index file.
func (idx *Index) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.file == nil {
		return nil
	}
	err := idx.file.Close()
	idx.file = nil
	return err
}
