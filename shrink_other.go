//go:build !unix

package buntdb

import (
	"os"
	"path/filepath"
)

// copyOwner reports false: this platform has no owner or group to copy.
func copyOwner(*os.File, os.FileInfo) bool { return false }

// syncDir does nothing: this platform cannot sync a directory.
func syncDir(string) error { return nil }

// absParent splits path into the absolute directory of the file and its base
// name. Windows normalizes ".." in the path text before it follows a symlink,
// so filepath.Abs, which cleans the text, names the right file. Abs also
// handles the rooted path \file and the drive-relative path C:file, which a
// working-directory prefix would break.
func absParent(path string) (dir, base string, err error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", "", err
	}
	return filepath.Dir(abs), filepath.Base(abs), nil
}
