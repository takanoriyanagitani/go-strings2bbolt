package strings2bbolt_test

import (
	"iter"
	"slices"
	"testing"

	s2b "github.com/takanoriyanagitani/go-strings2bbolt"
)

//nolint:gocognit,cyclop,funlen
func TestToChunked(t *testing.T) {
	t.Parallel()

	t.Run("ToChunked", func(t *testing.T) {
		t.Parallel()

		t.Run("single", func(t *testing.T) {
			t.Parallel()

			var batsz s2b.BatchSize = 1

			t.Run("empty", func(t *testing.T) {
				t.Parallel()

				var empty iter.Seq[uint32] = slices.Values([]uint32{})
				var chunked iter.Seq[[]uint32] = batsz.ToChunked(empty)
				var cnt uint64 = s2b.CountIter(chunked)
				if 0 != cnt {
					t.Errorf("expected empty iterator")
				}
			})

			t.Run("one", func(t *testing.T) {
				t.Parallel()

				var item1 iter.Seq[uint32] = slices.Values([]uint32{42})
				var chunked iter.Seq[[]uint32] = batsz.ToChunked(item1)
				var vals [][]uint32 = slices.Collect(chunked)
				if 1 != len(vals) {
					t.Fatalf("too many chunks")
				}

				var chunk1st []uint32 = vals[0]
				if 1 != len(chunk1st) {
					t.Fatalf("unexpected chunk")
				}

				var v1st uint32 = chunk1st[0]
				if 42 != v1st {
					t.Errorf("unexpected value got")
				}
			})

			t.Run("iii", func(t *testing.T) {
				t.Parallel()

				var item1 iter.Seq[uint32] = slices.Values([]uint32{634, 599, 3776})
				var chunked iter.Seq[[]uint32] = batsz.ToChunked(item1)
				var vals [][]uint32 = slices.Collect(chunked)
				if 3 != len(vals) {
					t.Fatalf("too many chunks")
				}
			})
		})

		t.Run("triple", func(t *testing.T) {
			t.Parallel()

			var batsz s2b.BatchSize = 3

			t.Run("empty", func(t *testing.T) {
				t.Parallel()

				var empty iter.Seq[uint32] = slices.Values([]uint32{})
				var chunked iter.Seq[[]uint32] = batsz.ToChunked(empty)
				var cnt uint64 = s2b.CountIter(chunked)
				if 0 != cnt {
					t.Errorf("expected empty iterator")
				}
			})

			t.Run("iii", func(t *testing.T) {
				t.Parallel()

				var item1 iter.Seq[uint32] = slices.Values([]uint32{634, 599, 3776})
				var chunked iter.Seq[[]uint32] = batsz.ToChunked(item1)
				var vals [][]uint32 = slices.Collect(chunked)
				if 1 != len(vals) {
					t.Fatalf("too many chunks")
				}

				var chnk1 []uint32 = vals[0]
				if 3 != len(chnk1) {
					t.Fatalf("too many items in a chunk")
				}

				var ok0 bool = 634 == chnk1[0]
				var ok1 bool = 599 == chnk1[1]
				var ok2 bool = 3776 == chnk1[2]

				var ok = ok0 && ok1 && ok2
				if !ok {
					t.Errorf("unexpected value got")
				}
			})

			t.Run("vii", func(t *testing.T) {
				t.Parallel()

				var item1 iter.Seq[uint32] = slices.Values([]uint32{
					634, 599, 3776,
					2, 3, 5, 7,
				})
				var chunked iter.Seq[[]uint32] = batsz.ToChunked(item1)
				var vals [][]uint32 = slices.Collect(chunked)
				if 3 != len(vals) {
					t.Fatalf("unexpected num of chunks")
				}

				var chnk1 []uint32 = vals[0]
				if 3 != len(chnk1) {
					t.Fatalf("too many items in a chunk")
				}

				if 3 != len(vals[1]) {
					t.Fatalf("too many items in a chunk")
				}

				if 1 != len(vals[2]) {
					t.Fatalf("too many items in a chunk")
				}
			})
		})
	})
}
