package buntdb

import (
	"fmt"
	"testing"
	"time"
)

// The spatial traversals check each item for expiry. These benchmarks measure
// that check on 10,000 2-D points on a 100x100 integer grid, inserted in key
// order. The TTL variant gives every item a TTL of one hour, so each search
// reads the clock once, on the first item, and compares deadlines on the rest.

// openSpatialGrid returns a database with the grid in the index "pts".
func openSpatialGrid(b *testing.B, ttl bool) *DB {
	b.Helper()
	db, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	var opts *SetOptions
	if ttl {
		opts = &SetOptions{Expires: true, TTL: time.Hour}
	}
	if err := db.Update(func(tx *Tx) error {
		if err := tx.CreateSpatialIndex("pts", "*", IndexRect); err != nil {
			return err
		}
		for i := 0; i < 100*100; i++ {
			if _, _, err := tx.Set(fmt.Sprintf("p:%05d", i), Point(float64(i/100), float64(i%100)), opts); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		b.Fatal(err)
	}
	return db
}

// runSpatialBench times a search in one read-only transaction. newSearch gets
// the callback counter, and builds the callback once, so the timed loop does
// not allocate it. The search runs once first, outside the timed loop, and must
// make want callbacks.
func runSpatialBench(b *testing.B, ttl bool, want int,
	newSearch func(n *int) func(tx *Tx) error) {
	db := openSpatialGrid(b, ttl)
	err := db.View(func(tx *Tx) error {
		var n int
		search := newSearch(&n)
		if err := search(tx); err != nil {
			return err
		}
		if n != want {
			return fmt.Errorf("search made %d callbacks, want %d", n, want)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			n = 0
			if err := search(tx); err != nil {
				return err
			}
		}
		b.StopTimer()
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	// Teardown runs outside the timed region.
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkSpatialIntersectsD1 searches a window with exactly 100 grid points.
// Bounds are inclusive: x and y in 20..29.
func BenchmarkSpatialIntersectsD1(b *testing.B) {
	for _, ttl := range []bool{false, true} {
		b.Run(fmt.Sprintf("ttl=%v", ttl), func(b *testing.B) {
			runSpatialBench(b, ttl, 100, func(n *int) func(tx *Tx) error {
				cb := func(string, string) bool { *n++; return true }
				return func(tx *Tx) error {
					return tx.Intersects("pts", "[19.5 19.5],[29.5 29.5]", cb)
				}
			})
		})
	}
}

// BenchmarkSpatialNearbyTopK reads the ten items nearest to the grid centre.
func BenchmarkSpatialNearbyTopK(b *testing.B) {
	const k = 10
	for _, ttl := range []bool{false, true} {
		b.Run(fmt.Sprintf("ttl=%v", ttl), func(b *testing.B) {
			runSpatialBench(b, ttl, k, func(n *int) func(tx *Tx) error {
				cb := func(string, string, float64) bool { *n++; return *n < k }
				return func(tx *Tx) error {
					return tx.Nearby("pts", "[49.5 49.5]", cb)
				}
			})
		})
	}
}
