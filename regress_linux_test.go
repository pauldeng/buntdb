package buntdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Linux fault injection uses a pipe for append errors and strace for sync
// errors on a real database file. No production filesystem abstraction is needed.

// redirectDBFile points db.file at a pipe with no reader, so writes fail with EPIPE.
func redirectDBFile(t *testing.T, db *DB) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Dup2(int(w.Fd()), int(db.file.Fd())); err != nil {
		t.Fatal(err)
	}
}

func openAlwaysSync(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "data.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.SetConfig(Config{SyncPolicy: Always, AutoShrinkDisabled: true}); err != nil {
		t.Fatal(err)
	}
	return db
}

// Control: a failing append is reported to the caller. This already works, and
// must keep working -- when A6b starts returning the sync error, that result
// must not mask or overwrite an earlier write error.
func TestControlAppendErrorIsReported(t *testing.T) {
	db := openAlwaysSync(t)
	redirectDBFile(t, db) // writes fail with EPIPE

	err := db.Update(func(tx *Tx) error {
		_, _, e := tx.Set("k", "v", nil)
		return e
	})
	if !errors.Is(err, syscall.EPIPE) {
		t.Fatalf("Update error = %v, want original append error EPIPE", err)
	}
	t.Logf("append failure correctly reported: %v", err)
}

// A6b: inject EIO into the second fsync while keeping actual file writes intact.
// A subprocess timeout bounds failures in Commit, View, and deferred Close.
func TestRegressSyncErrorIsReported(t *testing.T) {
	if os.Getenv("BUNTDB_SYNC_TEST_CHILD") == "1" {
		testSyncErrorOnFile(t)
		return
	}
	strace, err := exec.LookPath("strace")
	if err != nil {
		if os.Getenv("BUNTDB_REQUIRE_SYNC_TEST") == "1" {
			t.Fatal("strace is required for the Linux sync-error CI check")
		}
		t.Skip("sync-error injection requires strace; Linux CI requires this test")
	}
	tracePath := filepath.Join(t.TempDir(), "fsync.trace")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, strace, "-f", "-e", "trace=fsync",
		"-e", "inject=fsync:error=EIO:when=2", "-o", tracePath,
		os.Args[0], "-test.run=^TestRegressSyncErrorIsReported$", "-test.v", "-test.timeout=10s")
	cmd.Env = append(os.Environ(), "BUNTDB_SYNC_TEST_CHILD=1")
	// Kill both tracer and tracee if even the child's test timeout cannot finish.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	output, runErr := cmd.CombinedOutput()
	trace, traceErr := os.ReadFile(tracePath)
	if runErr != nil {
		t.Fatalf("sync-error subprocess: %v\n%s\n%s", runErr, output, trace)
	}
	if traceErr != nil {
		t.Fatal(traceErr)
	}
	if strings.Count(string(trace), "(INJECTED)") != 1 {
		t.Fatalf("want exactly one injected fsync failure, got:\n%s", trace)
	}
	t.Logf("real-file fsync injection and reopen checks executed:\n%s", output)
}

func testSyncErrorOnFile(t *testing.T) {
	t.Helper()
	// strace counts syscall occurrences per thread. Keep all foreground syncs
	// on one thread, and disable background shrinking in openAlwaysSync.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	db := openAlwaysSync(t)
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("before", "kept", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	var completed *Tx
	err := db.Update(func(tx *Tx) error {
		completed = tx
		_, _, err := tx.Set("during", "applied", nil)
		return err
	})
	var pathErr *os.PathError
	if !errors.Is(err, syscall.EIO) || !errors.As(err, &pathErr) || pathErr.Op != "sync" {
		t.Fatalf("Update error = %v, want injected sync EIO", err)
	}
	if completed == nil {
		t.Fatal("callback was not invoked")
	}
	if _, err := completed.Get("during"); err != ErrTxClosed {
		t.Errorf("transaction Get = %v, want ErrTxClosed", err)
	}
	if err := db.View(func(tx *Tx) error {
		for key, want := range map[string]string{"before": "kept", "during": "applied"} {
			if got, err := tx.Get(key); err != nil || got != want {
				return fmt.Errorf("after failed sync %q = %q, %v; want %q", key, got, err, want)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Continued writes are allowed, and the next sync succeeds.
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("after", "continued", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	path := db.file.Name()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.View(func(tx *Tx) error {
		for key, want := range map[string]string{
			"before": "kept", "during": "applied", "after": "continued",
		} {
			if got, err := tx.Get(key); err != nil || got != want {
				return fmt.Errorf("reopened %q = %q, %v; want %q", key, got, err, want)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
