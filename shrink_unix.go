//go:build unix

package buntdb

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// copyOwner gives dst the owner and group of the file that fi describes. It
// reports whether it did.
func copyOwner(dst *os.File, fi os.FileInfo) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && dst.Chown(int(st.Uid), int(st.Gid)) == nil
}

// syncDir makes a rename in directory dir durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// absParent splits path into the absolute directory of the file and its base
// name. It does not clean the directory. The kernel follows a symlink before it
// applies a "..", so base/link/../db is the file in the parent of link's
// target. filepath.Abs, Dir and Base would collapse the ".." first and name
// another file. A relative directory gets the working directory as a prefix.
func absParent(path string) (dir, base string, err error) {
	dir, base = ".", path
	if i := strings.LastIndexByte(path, '/'); i >= 0 {
		dir, base = path[:i+1], path[i+1:]
	}
	if !filepath.IsAbs(dir) {
		wd, err := os.Getwd()
		if err != nil {
			return "", "", err
		}
		dir = wd + "/" + dir
	}
	return dir, base, nil
}
