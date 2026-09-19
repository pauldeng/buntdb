package buntdb

// Regression coverage for the fixes in this series. TestControl* pin behaviour
// that already worked and must keep working; TestRegress* reproduce a defect and
// fail on the original code.

import (
	"testing"
)

// A7: CreateSpatialIndexOptions accepted an *IndexOptions and passed nil to
// createIndex, so CaseInsensitiveKeyMatching never took effect on a spatial
// index. Its sibling CreateIndexOptions passes opts correctly; commit 27d3577
// introduced both with that asymmetry.
//
// The b-tree half of this test is the control: it shows the option works there,
// so a failure is specific to the spatial path rather than to the option.
func TestRegressSpatialIndexOptionsHonoured(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		if err := tx.CreateSpatialIndexOptions("spatial", "POINT:*",
			&IndexOptions{CaseInsensitiveKeyMatching: true}, IndexRect); err != nil {
			return err
		}
		return tx.CreateIndexOptions("btree", "POINT:*",
			&IndexOptions{CaseInsensitiveKeyMatching: true}, IndexString)
	}); err != nil {
		t.Fatal(err)
	}

	// Key cased differently from the pattern. With the option honoured, both
	// indexes must pick it up.
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("point:1", Point(10, 10), nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	var spatial, btree int
	if err := db.View(func(tx *Tx) error {
		if err := tx.Intersects("spatial", Rect([]float64{0, 0}, []float64{20, 20}),
			func(k, v string) bool { spatial++; return true }); err != nil {
			return err
		}
		return tx.Ascend("btree", func(k, v string) bool { btree++; return true })
	}); err != nil {
		t.Fatal(err)
	}

	if btree != 1 {
		t.Fatalf("control failed: case-insensitive b-tree index holds %d items, want 1", btree)
	}
	if spatial != 1 {
		t.Errorf("case-insensitive spatial index holds %d items, want 1: "+
			"CaseInsensitiveKeyMatching is being discarded", spatial)
	}
}
