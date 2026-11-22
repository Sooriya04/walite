// Package: log
// File: segment_lifecycle.go
// Core Work: Segment disk synchronization, file closure, modification timestamp, and file unlinking
// Exported: none (methods on Segment: Sync, Close, IndexEntriesCount, ModTime, Remove)
package log

/*
Functions Manifest & Usage:
- (s *Segment) Sync() error: Flushes data file and index file buffers to physical disk.
- (s *Segment) Close() error: Syncs and closes data and index file descriptors safely.
- (s *Segment) IndexEntriesCount() uint32: Returns total sparse index entries in the segment index.
- (s *Segment) ModTime() time.Time: Returns modification time of the log file for retention checks.
- (s *Segment) Remove() error: Closes and unlinks both .log and .index files from disk.
*/

import (
	"os"
	"path/filepath"
	"time"
)

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

// ModTime returns the modification time of the underlying log file on disk.
func (s *Segment) ModTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.logFile != nil {
		if stat, err := s.logFile.Stat(); err == nil {
			return stat.ModTime()
		}
	}
	return time.Time{}
}

// Remove closes the segment and deletes both the .log and .index files from disk.
func (s *Segment) Remove() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var logPath, indexPath string
	if s.dir != "" {
		logPath = filepath.Join(s.dir, FormatSegmentFileName(s.baseOffset, ".log"))
		indexPath = filepath.Join(s.dir, FormatSegmentFileName(s.baseOffset, ".index"))
	} else if s.logFile != nil {
		logPath = s.logFile.Name()
		indexPath = filepath.Join(filepath.Dir(logPath), FormatSegmentFileName(s.baseOffset, ".index"))
	}
	if indexPath == "" && s.index != nil {
		indexPath = s.index.Path()
	}

	if !s.closed {
		s.closed = true
		if s.logFile != nil {
			_ = s.logFile.Close()
		}
		if s.index != nil {
			_ = s.index.Close()
		}
	}

	var firstErr error
	if logPath != "" {
		if err := os.Remove(logPath); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	if indexPath != "" {
		if err := os.Remove(indexPath); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
