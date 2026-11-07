//go:build !unix

package buntdb

import "os"

// copyOwner reports false: this platform has no owner or group to copy.
func copyOwner(*os.File, os.FileInfo) bool { return false }

// syncDir does nothing: this platform cannot sync a directory.
func syncDir(string) error { return nil }
