package buntdb

// Regression coverage for the fixes in this series. TestControl* pin behaviour
// that already worked and must keep working; TestRegress* reproduce a defect and
// fail on the original code.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// A3: readLoad indexed parts[0][1] and parts[0][2] after checking only
// len(parts) == 0, so a command name shorter than three characters panicked
// inside Open. Commit f240a9c guarded this with a length check; b559e28
// removed it to admit the seven-character "flushdb".
func TestRegressShortCommandName(t *testing.T) {
	for _, tc := range []struct {
		name string
		aof  string
	}{
		{"empty name", "*1\r\n$0\r\n\r\n"},
		{"one char", "*1\r\n$1\r\ns\r\n"},
		{"two chars", "*1\r\n$2\r\nse\r\n"},
		{"prefix only", "*3\r\n$4\r\nsetx\r\n$1\r\na\r\n$1\r\n1\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.db")
			if err := os.WriteFile(path, []byte(tc.aof), 0666); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Open panicked on a malformed command name: %v", r)
				}
			}()
			db, err := Open(path)
			if db != nil {
				defer db.Close()
			}
			if err != ErrInvalid {
				t.Fatalf("Open = %v, want ErrInvalid", err)
			}
		})
	}
}

// Command names are matched case-insensitively. This guards the A3 length-check
// restoration against accidentally making the parser case-sensitive.
func TestControlCommandNameCase(t *testing.T) {
	for _, tc := range []struct {
		name string
		aof  string
		key  string
		want string // "" means the key must be absent
	}{
		{"set lower", "*3\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\n1\r\n", "a", "1"},
		{"set upper", "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n", "a", "1"},
		{"set mixed", "*3\r\n$3\r\nSeT\r\n$1\r\na\r\n$1\r\n1\r\n", "a", "1"},
		{"del lower", "*3\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$3\r\ndel\r\n$1\r\na\r\n", "a", ""},
		{"del upper", "*3\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$3\r\nDEL\r\n$1\r\na\r\n", "a", ""},
		{"flushdb lower", "*3\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\n1\r\n*1\r\n$7\r\nflushdb\r\n", "a", ""},
		{"flushdb upper", "*3\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\n1\r\n*1\r\n$7\r\nFLUSHDB\r\n", "a", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "data.db")
			if err := os.WriteFile(path, []byte(tc.aof), 0666); err != nil {
				t.Fatal(err)
			}
			db, err := Open(path)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer db.Close()
			if err := db.View(func(tx *Tx) error {
				v, err := tx.Get(tc.key)
				if tc.want == "" {
					if err != ErrNotFound {
						return fmt.Errorf("key %q = %q (%v), want ErrNotFound", tc.key, v, err)
					}
					return nil
				}
				if err != nil {
					return fmt.Errorf("key %q: %w", tc.key, err)
				}
				if v != tc.want {
					return fmt.Errorf("key %q = %q, want %q", tc.key, v, tc.want)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// RESP command names are ASCII; Unicode case folding must not turn malformed
// input into a command, particularly a destructive FLUSHDB.
func TestControlASCIICommandNames(t *testing.T) {
	for _, command := range []string{"ſet", "fluſhdb"} {
		t.Run(command, func(t *testing.T) {
			db, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			aof := fmt.Sprintf("*3\r\n$%d\r\n%s\r\n$1\r\na\r\n$1\r\nb\r\n", len(command), command)
			if command == "fluſhdb" {
				aof = fmt.Sprintf("*1\r\n$%d\r\n%s\r\n", len(command), command)
			}
			if err := db.Load(strings.NewReader(aof)); err != ErrInvalid {
				t.Fatalf("Load(%q) = %v, want ErrInvalid", command, err)
			}
		})
	}
}
