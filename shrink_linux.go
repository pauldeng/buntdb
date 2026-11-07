//go:build linux

package buntdb

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"syscall"
)

const accessACL = "system.posix_acl_access"

// fdPath names an open file. A /proc/self/fd path cannot be redirected by a
// swapped directory entry. The caller must keep f alive while it uses the path.
func fdPath(f *os.File) string { return fmt.Sprintf("/proc/self/fd/%d", f.Fd()) }

// copyACL makes the POSIX access ACL of dst equal to that of src. If src has
// none, it removes the ACL that dst inherited from its directory. It does
// nothing when the file system has no ACLs.
func copyACL(dst, src *os.File) error {
	defer runtime.KeepAlive(src)
	defer runtime.KeepAlive(dst)
	from, to := fdPath(src), fdPath(dst)
	n, err := syscall.Getxattr(from, accessACL, nil)
	var acl []byte
	if err == nil {
		acl = make([]byte, n)
		n, err = syscall.Getxattr(from, accessACL, acl)
		acl = acl[:max(n, 0)]
	}
	switch {
	case err == nil:
		err = syscall.Setxattr(to, accessACL, acl, 0)
	case errors.Is(err, syscall.ENODATA):
		return removeACL(dst)
	}
	if errors.Is(err, syscall.ENOTSUP) {
		return nil
	}
	return err
}

// removeACL removes the access ACL that dst inherited from its directory. It
// succeeds when dst has none, or when the file system has no ACLs.
func removeACL(dst *os.File) error {
	defer runtime.KeepAlive(dst)
	err := syscall.Removexattr(fdPath(dst), accessACL)
	if errors.Is(err, syscall.ENODATA) || errors.Is(err, syscall.ENOTSUP) {
		return nil
	}
	return err
}
