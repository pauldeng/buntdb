//go:build unix

package buntdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// S1: a ".." that follows a symlink names the parent of the link's target,
// not the parent of the link. Open must record the file that it opens. base/link
// leads to other/sub, so base/link/../db is other/db. The file base/db is a
// different file, and Shrink must not touch it.
func TestRegressDBPathDotDotAfterSymlink(t *testing.T) {
	for _, tc := range []struct{ cleanedExists, relative bool }{
		{false, false}, {true, false}, {false, true}, {true, true},
	} {
		cleanedExists, relative := tc.cleanedExists, tc.relative
		t.Run(fmt.Sprintf("cleaned path exists=%v/relative=%v", cleanedExists, relative), func(t *testing.T) {
			base, other := t.TempDir(), t.TempDir()
			if err := os.Mkdir(filepath.Join(other, "sub"), 0777); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(other, "sub"), filepath.Join(base, "link")); err != nil {
				t.Skipf("symlink: %v", err)
			}
			sep := string(filepath.Separator)
			const sentinel = "not a database\r\n"
			if cleanedExists {
				if err := os.WriteFile(filepath.Join(base, "db"), []byte(sentinel), 0666); err != nil {
					t.Fatal(err)
				}
			}
			// Join would clean the "..". A relative path takes the other branch of Open.
			path := base + sep + "link" + sep + ".." + sep + "db"
			if relative {
				t.Chdir(base)
				path = "link" + sep + ".." + sep + "db"
			}
			db, err := Open(path)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer db.Close()
			if err := setTTLKey(db, "k"); err != nil {
				t.Fatal(err)
			}
			if err := db.Shrink(); err != nil {
				t.Fatal(err)
			}
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			realOther, _ := filepath.EvalSymlinks(other)
			if want := filepath.Join(realOther, "db"); db.path != want || db.dir != realOther {
				t.Fatalf("db.path = %q, db.dir = %q; want %q in %q", db.path, db.dir, want, realOther)
			}
			if fi, err := os.Stat(filepath.Join(other, "db")); err != nil || fi.Size() == 0 {
				t.Fatalf("the database file other/db: %v", err)
			}
			got, err := os.ReadFile(filepath.Join(base, "db"))
			switch {
			case cleanedExists && (err != nil || string(got) != sentinel):
				t.Fatalf("base/db was changed: %q, %v", got, err)
			case !cleanedExists && err == nil:
				t.Fatal("base/db was created")
			}
		})
	}
}
