//go:build !linux

package buntdb

import "os"

// copyACL does nothing: only Linux has POSIX ACLs to copy.
func copyACL(dst, src *os.File) error { return nil }

// removeACL does nothing: only Linux has POSIX ACLs to remove.
func removeACL(dst *os.File) error { return nil }
