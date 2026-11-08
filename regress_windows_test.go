//go:build windows

package buntdb

import (
	"os"
	"path/filepath"
	"testing"
)

// S1: absParent on Windows must handle the rooted path \file and the
// drive-relative path C:file. A working-directory prefix would turn them into
// C:\work\C:file and C:\work\file.
func TestRegressAbsParentWindows(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	vol := filepath.VolumeName(cwd)
	for _, tc := range []struct{ path, dir, base string }{
		{`\tick.db`, vol + `\`, "tick.db"},        // rooted: the root of the current drive
		{vol + `tick.db`, cwd, "tick.db"},         // drive-relative: relative to the cwd of that drive
		{`sub\..\tick.db`, cwd, "tick.db"},        // Windows cleans the ".." in the text
		{filepath.Join(cwd, "x.db"), cwd, "x.db"}, // absolute
		{"tick.db", cwd, "tick.db"},               // relative
	} {
		dir, base, err := absParent(tc.path)
		if err != nil {
			t.Fatalf("absParent(%q): %v", tc.path, err)
		}
		if !sameDir(dir, tc.dir) || base != tc.base {
			t.Errorf("absParent(%q) = %q, %q; want %q, %q", tc.path, dir, base, tc.dir, tc.base)
		}
	}
}

func sameDir(a, b string) bool {
	fa, errA := os.Stat(a)
	fb, errB := os.Stat(b)
	return errA == nil && errB == nil && os.SameFile(fa, fb)
}

// S1: Open with a drive-relative path creates the file in the working
// directory of that drive.
func TestRegressOpenDriveRelativeWindows(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	db, err := Open(filepath.VolumeName(cwd) + "drive.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := setTTLKey(db, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(cwd, "drive.db")); err != nil {
		t.Fatalf("drive-relative Open did not use the working directory: %v", err)
	}
}
