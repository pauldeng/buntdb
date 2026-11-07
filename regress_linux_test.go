package buntdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
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

// runUnderStrace re-runs the named test in a subprocess with
// BUNTDB_SYNC_TEST_CHILD=1 under strace, and returns the trace of the events.
// inject is a strace -e inject expression. A subprocess timeout bounds hangs.
func runUnderStrace(t *testing.T, test, inject, events string) string {
	t.Helper()
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
	cmd := exec.CommandContext(ctx, strace, "-f", "-e", "signal=none", "-e", "trace="+events,
		"-e", "inject="+inject, "-o", tracePath,
		os.Args[0], "-test.run=^"+test+"$", "-test.v", "-test.timeout=10s")
	cmd.Env = append(os.Environ(), "BUNTDB_SYNC_TEST_CHILD=1")
	// Kill both tracer and tracee if even the child's test timeout cannot finish.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	output, runErr := cmd.CombinedOutput()
	trace, traceErr := os.ReadFile(tracePath)
	if runErr != nil {
		t.Fatalf("subprocess %s: %v\n%s\n%s", test, runErr, output, trace)
	}
	if traceErr != nil {
		t.Fatal(traceErr)
	}
	t.Logf("subprocess output:\n%s", output)
	return string(trace)
}

// joinStraceLines returns the trace with each syscall on one line. With -f,
// strace may split a call into "pid name(args <unfinished ...>" and
// "pid <... name resumed>) = result"; this joins the two halves by pid.
func joinStraceLines(trace string) []string {
	var out []string
	pending := map[string]string{} // pid -> first half
	for _, line := range strings.Split(trace, "\n") {
		line = strings.TrimLeft(line, " ") // strace pads pids to a common width
		pid, rest, _ := strings.Cut(line, " ")
		if i := strings.Index(rest, "<unfinished ...>"); i >= 0 {
			pending[pid] = pid + " " + strings.TrimRight(rest[:i], " ")
			continue
		}
		if strings.HasPrefix(rest, "<... ") {
			if _, after, ok := strings.Cut(rest, " resumed>"); ok {
				line = pending[pid] + after
				delete(pending, pid)
			}
		}
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

func TestJoinStraceLines(t *testing.T) {
	trace := "10 fsync(3 <unfinished ...>\n" +
		"11 unlinkat(AT_FDCWD, \"x\", 0) = -1 ENOENT\n" +
		"10 <... fsync resumed>)        = -1 EIO (INJECTED)\n" +
		"10 fsync(3)                    = 0\n" +
		"  9 fsync(3 <unfinished ...>\n" + // padded pid, as strace prints it
		" 9 <... fsync resumed>)         = 0\n"
	want := []string{
		"11 unlinkat(AT_FDCWD, \"x\", 0) = -1 ENOENT",
		"10 fsync(3)        = -1 EIO (INJECTED)",
		"10 fsync(3)                    = 0",
		"9 fsync(3)         = 0",
	}
	if got := joinStraceLines(trace); !slices.Equal(got, want) {
		t.Errorf("joinStraceLines = %q, want %q", got, want)
	}
}

// A6b: inject EIO into the second fsync while keeping actual file writes intact.
func TestRegressSyncErrorIsReported(t *testing.T) {
	if os.Getenv("BUNTDB_SYNC_TEST_CHILD") == "1" {
		testSyncErrorOnFile(t)
		return
	}
	trace := runUnderStrace(t, "TestRegressSyncErrorIsReported", "fsync:error=EIO:when=2", "fsync")
	if strings.Count(trace, "(INJECTED)") != 1 {
		t.Fatalf("want exactly one injected fsync failure, got:\n%s", trace)
	}
}

// D2: Close ignored the error of its final fsync. It must release everything
// and then return the error.
func TestRegressCloseReportsSyncError(t *testing.T) {
	if os.Getenv("BUNTDB_SYNC_TEST_CHILD") != "1" {
		trace := runUnderStrace(t, "TestRegressCloseReportsSyncError", "fsync:error=EIO:when=1", "fsync")
		if strings.Count(trace, "(INJECTED)") != 1 {
			t.Fatalf("want exactly one injected fsync failure, got:\n%s", trace)
		}
		return
	}
	// SyncPolicy Never and LockOSThread make this Close the first fsync on
	// its thread, so it is the one that is injected.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	path := filepath.Join(t.TempDir(), "data.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetConfig(Config{SyncPolicy: Never, AutoShrinkDisabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("k", "v", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Close = %v, want the injected sync EIO", err)
	}
	if err := db.Close(); err != ErrDatabaseClosed {
		t.Errorf("second Close = %v, want ErrDatabaseClosed", err)
	}
	if err := db.View(func(tx *Tx) error { return nil }); err != ErrDatabaseClosed {
		t.Errorf("View after Close = %v, want ErrDatabaseClosed", err)
	}
	if db.keys != nil || db.exps != nil || db.idxs != nil || db.file != nil {
		t.Error("Close did not release its internal references")
	}
	// The file descriptor is released.
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		if target, err := os.Readlink("/proc/self/fd/" + fd.Name()); err == nil && target == path {
			t.Errorf("fd %s still refers to the database file", fd.Name())
		}
	}
	// The data was appended before the failed sync, so it is readable.
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.View(func(tx *Tx) error {
		if v, err := tx.Get("k"); err != nil || v != "v" {
			return fmt.Errorf("reopened k = %q, %v", v, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil { // the second fsync on this thread is not injected
		t.Fatal(err)
	}
}

// C11: a failed background fsync must be retried while the DB is idle. The
// fsync before the marker is the background manager's; the fsync after it is
// Close's. The first fsync fails, so a retry is a second fsync before the marker.
func TestRegressBackgroundSyncRetried(t *testing.T) {
	const marker = "buntdb-idle-end"
	if os.Getenv("BUNTDB_SYNC_TEST_CHILD") != "1" {
		trace := runUnderStrace(t, "TestRegressBackgroundSyncRetried", "fsync:error=EIO:when=1", "fsync,unlinkat")
		var syncs []string
		found := false
		for _, line := range joinStraceLines(trace) {
			if strings.Contains(line, marker) {
				found = true
				break
			}
			if strings.Contains(line, "fsync(") {
				syncs = append(syncs, line)
			}
		}
		if !found {
			t.Fatalf("no marker in trace:\n%s", trace)
		}
		if len(syncs) == 0 || !strings.Contains(syncs[0], "(INJECTED)") {
			t.Fatalf("the first background fsync was not injected:\n%s", trace)
		}
		if len(syncs) < 2 {
			t.Fatalf("failed background fsync was not retried while idle:\n%s", trace)
		}
		// Once a sync succeeds, the idle database needs no more. A failure may
		// repeat when the retry runs on another thread, where strace injects again.
		// The last sync need not succeed: every retry may land on a fresh thread.
		for _, s := range syncs[:len(syncs)-1] {
			if !strings.Contains(s, "(INJECTED)") {
				t.Fatalf("background fsync ran again after a successful one:\n%s", trace)
			}
		}
		return
	}
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "data.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetConfig(Config{SyncPolicy: EverySecond, AutoShrinkDisabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("k", "v", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5500 * time.Millisecond) // five background ticks, no new writes
	// The marker shows up in the trace. Holding db.mu keeps it out of a background fsync.
	db.mu.Lock()
	_ = os.Remove(filepath.Join(dir, marker))
	db.mu.Unlock()
	if err := db.Close(); err != nil && !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
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
