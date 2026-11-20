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
// an append-only, fixed-size entry index file on disk, backed by an in-memory
// entry cache for zero-syscall lookups.
type Index struct {
	mu      sync.RWMutex
	file    *os.File
	entries []IndexEntry
}

// OpenIndex opens or creates an index file at the specified path.
// It validates that existing files have sizes aligned to 8-byte boundaries and loads entries into memory.
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
	if remainder := fileSize % EntrySize; remainder != 0 {
		alignedSize := fileSize - remainder
		if err := file.Truncate(alignedSize); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("failed to truncate torn index entry: %w", err)
		}
		fileSize = alignedSize
	}

	count := fileSize / EntrySize
	entries := make([]IndexEntry, count)
	if count > 0 {
		buf := make([]byte, fileSize)
		if _, err := file.ReadAt(buf, 0); err != nil && err != io.EOF {
			_ = file.Close()
			return nil, fmt.Errorf("failed to read index entries into memory: %w", err)
		}
		for i := int64(0); i < count; i++ {
			off := i * EntrySize
			entries[i] = IndexEntry{
				RelativeOffset: binary.BigEndian.Uint32(buf[off : off+4]),
				Position:       binary.BigEndian.Uint32(buf[off+4 : off+8]),
			}
		}
	}

	return &Index{
		file:    file,
		entries: entries,
	}, nil
}

// EntriesCount returns the total number of entries currently stored in the index.
func (idx *Index) EntriesCount() uint32 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return uint32(len(idx.entries))
}

// WriteEntry appends a new (relativeOffset, position) entry to the index file.
// It verifies that relativeOffset is monotonically non-decreasing.
func (idx *Index) WriteEntry(relativeOffset, position uint32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.file == nil {
		return os.ErrClosed
	}

	if len(idx.entries) > 0 && relativeOffset < idx.entries[len(idx.entries)-1].RelativeOffset {
		return errors.New("walite: non-monotonic index relative offset")
	}

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

	idx.entries = append(idx.entries, IndexEntry{
		RelativeOffset: relativeOffset,
		Position:       position,
	})
	return nil
}

// ReadAt reads the index entry at the given entry index (0-indexed).
func (idx *Index) ReadAt(entryIdx uint32) (IndexEntry, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.file == nil {
		return IndexEntry{}, os.ErrClosed
	}

	if int(entryIdx) >= len(idx.entries) {
		return IndexEntry{}, io.EOF
	}
	return idx.entries[entryIdx], nil
}

// Lookup performs a binary search over the cached in-memory index to locate the highest
// index entry whose RelativeOffset is <= targetRelOffset in O(log N) user-space steps without disk I/O.
func (idx *Index) Lookup(targetRelOffset uint32) (IndexEntry, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.file == nil {
		return IndexEntry{}, os.ErrClosed
	}

	n := len(idx.entries)
	if n == 0 || targetRelOffset < idx.entries[0].RelativeOffset {
		return IndexEntry{}, ErrIndexOutOfBounds
	}

	low := 0
	high := n - 1
	bestIdx := 0

	for low <= high {
		mid := low + (high-low)/2
		if idx.entries[mid].RelativeOffset <= targetRelOffset {
			bestIdx = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return idx.entries[bestIdx], nil
}

// Truncate resizes the index to contain exactly count entries, truncating physical file size and memory cache.
func (idx *Index) Truncate(count uint32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.file == nil {
		return os.ErrClosed
	}

	newBytes := int64(count) * EntrySize
	if err := idx.file.Truncate(newBytes); err != nil {
		return fmt.Errorf("failed to truncate index file: %w", err)
	}

	if _, err := idx.file.Seek(newBytes, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek index after truncate: %w", err)
	}

	if int(count) < len(idx.entries) {
		idx.entries = idx.entries[:count]
	}
	return nil
}

// Sync flushes file changes to disk.
func (idx *Index) Sync() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.file == nil {
		return os.ErrClosed
	}

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
