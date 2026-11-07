//go:build unix

package buntdb

import (
	"os"
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
