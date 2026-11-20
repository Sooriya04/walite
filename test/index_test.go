// Package: test
// File: index_test.go
// Core Work: Validation of fixed-size sparse indexing, binary search lookups, boundary conditions, and file recovery
// Exported: None
package test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/barbaravane/walite/internal/log"
)

func TestIndex_WriteAndReadEntries(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "00000000000000000000.index")

	idx, err := log.OpenIndex(indexPath)
	if err != nil {
		t.Fatalf("unexpected error opening index: %v", err)
	}
	defer idx.Close()

	if count := idx.EntriesCount(); count != 0 {
		t.Fatalf("expected 0 entries initially, got %d", count)
	}

	// Write sequential index entries
	testData := []log.IndexEntry{
		{RelativeOffset: 0, Position: 0},
		{RelativeOffset: 5, Position: 4096},
		{RelativeOffset: 12, Position: 8192},
		{RelativeOffset: 30, Position: 16384},
	}

	for _, entry := range testData {
		if err := idx.WriteEntry(entry.RelativeOffset, entry.Position); err != nil {
			t.Fatalf("failed to write entry: %v", err)
		}
	}

	if count := idx.EntriesCount(); count != uint32(len(testData)) {
		t.Fatalf("expected %d entries, got %d", len(testData), count)
	}

	for i, expected := range testData {
		got, err := idx.ReadAt(uint32(i))
		if err != nil {
			t.Fatalf("failed to read entry at index %d: %v", i, err)
		}
		if got != expected {
			t.Fatalf("entry %d mismatch: got %+v, want %+v", i, got, expected)
		}
	}
}

func TestIndex_BinarySearchLookup(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "lookup.index")

	idx, err := log.OpenIndex(indexPath)
	if err != nil {
		t.Fatalf("unexpected error opening index: %v", err)
	}
	defer idx.Close()

	// Empty index lookup
	if _, err := idx.Lookup(0); err != log.ErrIndexOutOfBounds {
		t.Fatalf("expected ErrIndexOutOfBounds for empty index, got %v", err)
	}

	entries := []log.IndexEntry{
		{RelativeOffset: 10, Position: 1024},
		{RelativeOffset: 20, Position: 2048},
		{RelativeOffset: 30, Position: 4096},
		{RelativeOffset: 50, Position: 8192},
		{RelativeOffset: 100, Position: 16384},
	}

	for _, entry := range entries {
		if err := idx.WriteEntry(entry.RelativeOffset, entry.Position); err != nil {
			t.Fatalf("failed to write entry: %v", err)
		}
	}

	// Target before first entry
	if _, err := idx.Lookup(5); err != log.ErrIndexOutOfBounds {
		t.Fatalf("expected ErrIndexOutOfBounds for target 5, got %v", err)
	}

	// Exact matches
	cases := []struct {
		target   uint32
		expected log.IndexEntry
	}{
		{target: 10, expected: log.IndexEntry{RelativeOffset: 10, Position: 1024}},
		{target: 15, expected: log.IndexEntry{RelativeOffset: 10, Position: 1024}},
		{target: 20, expected: log.IndexEntry{RelativeOffset: 20, Position: 2048}},
		{target: 25, expected: log.IndexEntry{RelativeOffset: 20, Position: 2048}},
		{target: 30, expected: log.IndexEntry{RelativeOffset: 30, Position: 4096}},
		{target: 49, expected: log.IndexEntry{RelativeOffset: 30, Position: 4096}},
		{target: 50, expected: log.IndexEntry{RelativeOffset: 50, Position: 8192}},
		{target: 75, expected: log.IndexEntry{RelativeOffset: 50, Position: 8192}},
		{target: 100, expected: log.IndexEntry{RelativeOffset: 100, Position: 16384}},
		{target: 9999, expected: log.IndexEntry{RelativeOffset: 100, Position: 16384}},
	}

	for _, tc := range cases {
		got, err := idx.Lookup(tc.target)
		if err != nil {
			t.Fatalf("lookup failed for target %d: %v", tc.target, err)
		}
		if got != tc.expected {
			t.Fatalf("lookup(%d) got %+v, want %+v", tc.target, got, tc.expected)
		}
	}
}

func TestIndex_RecoveryAndCorruptCheck(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "recover.index")

	// 1. Write some valid entries and close
	idx, err := log.OpenIndex(indexPath)
	if err != nil {
		t.Fatalf("failed to open index: %v", err)
	}
	_ = idx.WriteEntry(0, 0)
	_ = idx.WriteEntry(10, 1024)
	if err := idx.Close(); err != nil {
		t.Fatalf("failed to close index: %v", err)
	}

	// 2. Re-open index, verify entry count
	idx2, err := log.OpenIndex(indexPath)
	if err != nil {
		t.Fatalf("failed to re-open valid index: %v", err)
	}
	if count := idx2.EntriesCount(); count != 2 {
		t.Fatalf("expected 2 entries on re-open, got %d", count)
	}
	_ = idx2.Close()

	// 3. Corrupt file by appending an incomplete entry (e.g. 5 bytes instead of 8)
	f, err := os.OpenFile(indexPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open file for corrupt append: %v", err)
	}
	if _, err := f.Write([]byte{1, 2, 3, 4, 5}); err != nil {
		t.Fatalf("failed to append partial bytes: %v", err)
	}
	_ = f.Close()

	// 4. Opening corrupt index must return ErrIndexCorrupt
	_, err = log.OpenIndex(indexPath)
	if err != log.ErrIndexCorrupt {
		t.Fatalf("expected ErrIndexCorrupt, got %v", err)
	}
}

func TestIndex_Truncate(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "trunc.index")

	idx, err := log.OpenIndex(indexPath)
	if err != nil {
		t.Fatalf("failed to open index: %v", err)
	}
	defer idx.Close()

	for i := uint32(0); i < 10; i++ {
		if err := idx.WriteEntry(i*10, i*1000); err != nil {
			t.Fatalf("failed to write entry %d: %v", i, err)
		}
	}
	if count := idx.EntriesCount(); count != 10 {
		t.Fatalf("expected 10 entries, got %d", count)
	}

	// Truncate down to 4 entries
	if err := idx.Truncate(4); err != nil {
		t.Fatalf("failed to truncate index: %v", err)
	}
	if count := idx.EntriesCount(); count != 4 {
		t.Fatalf("expected 4 entries after truncate, got %d", count)
	}

	// Check 5th entry returns EOF
	if _, err := idx.ReadAt(4); err == nil {
		t.Fatalf("expected error reading truncated entry, got nil")
	}

	// Verify we can still write again after truncate
	if err := idx.WriteEntry(40, 4000); err != nil {
		t.Fatalf("failed to write entry after truncate: %v", err)
	}
	if count := idx.EntriesCount(); count != 5 {
		t.Fatalf("expected 5 entries, got %d", count)
	}
}

func BenchmarkIndex_Lookup(b *testing.B) {
	tempDir := b.TempDir()
	indexPath := filepath.Join(tempDir, "bench.index")
	idx, err := log.OpenIndex(indexPath)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	// Seed 10,000 entries
	for i := uint32(0); i < 10000; i++ {
		if err := idx.WriteEntry(i*10, i*4096); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		target := uint32((i % 10000) * 10)
		_, _ = idx.Lookup(target)
	}
}
