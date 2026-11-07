package buntdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
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
// inject is a space-separated list of strace -e inject expressions, or empty.
// Fd arguments in the trace show their paths (-y). A subprocess
// timeout bounds hangs. extra holds more strace options.
func runUnderStrace(t *testing.T, test, inject, events string, extra ...string) string {
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
	args := []string{"-f", "-y", "-e", "signal=none", "-e", "trace=" + events}
	for _, in := range strings.Fields(inject) {
		args = append(args, "-e", "inject="+in)
	}
	args = append(args, extra...)
	args = append(args, "-o", tracePath, os.Args[0], "-test.run=^"+test+"$", "-test.v", "-test.timeout=10s")
	cmd := exec.CommandContext(ctx, strace, args...)
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

// shrinkFixture opens a database in a new directory with a log that is longer
// than its state, so that Shrink changes the file. It returns the log bytes.
// Writes are not synced, so the first fsync on the thread is Shrink's.
func shrinkFixture(t *testing.T) (db *DB, path string, orig []byte) {
	t.Helper()
	dir := os.Getenv("BUNTDB_SHRINK_DIR") // set by runShrinkCase, so that strace can filter on the path
	if dir == "" {
		dir = t.TempDir()
	}
	path = filepath.Join(dir, "s.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.SetConfig(Config{SyncPolicy: Never, AutoShrinkDisabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		for _, kv := range [][2]string{{"a", "1"}, {"a", "2"}, {"b", "3"}, {"c", "4"}} {
			if _, _, err := tx.Set(kv[0], kv[1], nil); err != nil {
				return err
			}
		}
		_, err := tx.Delete("c")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if orig, err = os.ReadFile(path); err != nil {
		t.Fatal(err)
	}
	return db, path, orig
}

// wantKeys fails unless the database holds exactly want.
func wantKeys(t *testing.T, db *DB, want map[string]string) {
	t.Helper()
	got := map[string]string{}
	if err := db.View(func(tx *Tx) error {
		return tx.Ascend("", func(k, v string) bool { got[k] = v; return true })
	}); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(got, want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
}

// assertNoOpenFiles fails if this process holds a file descriptor for a file
// in dir. That would be a leaked database or temp file.
func assertNoOpenFiles(t *testing.T, dir string) {
	t.Helper()
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		if target, err := os.Readlink("/proc/self/fd/" + fd.Name()); err == nil && strings.HasPrefix(target, dir+"/") {
			t.Errorf("fd %s still refers to %s", fd.Name(), target)
		}
	}
}

// closeInheritedFDsOnExec keeps fds 3 and up, such as a make jobserver pipe
// or a shell redirection, out of the strace child. The runtime's own fds
// already have close-on-exec set.
func closeInheritedFDsOnExec() {
	entries, _ := os.ReadDir("/proc/self/fd") // on error, the test's own checks fail
	for _, e := range entries {
		if fd, err := strconv.Atoi(e.Name()); err == nil && fd > 2 {
			syscall.CloseOnExec(fd)
		}
	}
}

// shrinkCase is one Shrink failure state (REVIEW_PLAN section 3).
type shrinkCase struct {
	inject  string   // strace -e inject expressions
	events  string   // strace trace set: must include every call in inject
	filter  string   // "db": count only calls on s.db; "fd6": only calls on fd 6; "": all calls
	hits    []string // a regexp for each injected trace line, in order
	err     error    // the error Shrink must wrap
	alsoErr error    // a second error Shrink must wrap, or nil
	newFile bool     // the disk holds the shrunk file, not the original log
	open    bool     // the database is still usable; otherwise it is closed
	leaks   bool     // the injected close skips the real close, so its fd stays open
}

// runShrinkCase runs one failure state in a strace subprocess. The child runs
// Shrink on a thread that it locks. strace counts calls per thread, and a
// thread has its own history of calls from before the test started. So a
// when=N that counts every call would depend on which thread the test got.
// The filter field makes N count only calls on the database file (-P) or on
// the temp file's fd (6: the child opens 5 for s.db, and ReadFile's fd 6 is
// closed before Shrink). Those calls all happen in the locked child, after
// LockOSThread. The parent marks its inherited fds close-on-exec, so those
// numbers do not move when a caller leaves extra fds open. The parent also checks that each injected line is the
// intended call, so a miscount fails the test instead of passing it.
func runShrinkCase(t *testing.T, test string, c shrinkCase) {
	if os.Getenv("BUNTDB_SYNC_TEST_CHILD") != "1" {
		dir := t.TempDir()
		t.Setenv("BUNTDB_SHRINK_DIR", dir)
		closeInheritedFDsOnExec()
		var extra []string
		switch c.filter {
		case "db":
			extra = []string{"-P", filepath.Join(dir, "s.db")}
		case "fd6":
			extra = []string{"-e", "trace-fds=6"}
		}
		trace := runUnderStrace(t, test, c.inject, c.events, extra...)
		var injected []string
		for _, line := range joinStraceLines(trace) {
			if strings.Contains(line, "(INJECTED)") {
				injected = append(injected, line)
			}
		}
		if len(injected) != len(c.hits) {
			t.Fatalf("injected %d calls, want %d:\n%s", len(injected), len(c.hits), trace)
		}
		for i, hit := range c.hits {
			if !regexp.MustCompile(hit).MatchString(injected[i]) {
				t.Fatalf("injected call %q does not match %q:\n%s", injected[i], hit, trace)
			}
		}
		return
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	db, path, orig := shrinkFixture(t)
	dir := filepath.Dir(path)
	state := map[string]string{"a": "2", "b": "3"}

	if err := db.Shrink(); !errors.Is(err, c.err) || c.alsoErr != nil && !errors.Is(err, c.alsoErr) {
		t.Fatalf("Shrink = %v, want %v and %v", err, c.err, c.alsoErr)
	}
	// Disk: the original log or the complete new file, and no temp file.
	if data, err := os.ReadFile(path); err != nil {
		t.Fatal(err)
	} else if c.newFile != (len(data) < len(orig)) || bytes.Equal(data, orig) == c.newFile {
		t.Errorf("disk holds %q, original %q, want the %s file", data, orig,
			map[bool]string{false: "original", true: "shrunk"}[c.newFile])
	}
	if names := dirNames(t, dir); !slices.Equal(names, []string{"s.db"}) {
		t.Errorf("directory holds %v, want only s.db", names)
	}

	if c.open {
		// The writer is at the end of the file: a new commit follows the old data.
		if err := db.Update(func(tx *Tx) error {
			_, _, err := tx.Set("after", "x", nil)
			return err
		}); err != nil {
			t.Fatalf("Update after failed Shrink: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		state["after"] = "x"
	} else {
		if err := db.Update(func(tx *Tx) error { return nil }); err != ErrDatabaseClosed {
			t.Errorf("Update = %v, want ErrDatabaseClosed", err)
		}
		if err := db.Shrink(); err != ErrDatabaseClosed {
			t.Errorf("second Shrink = %v, want ErrDatabaseClosed", err)
		}
		if err := db.Close(); err != ErrDatabaseClosed {
			t.Errorf("Close = %v, want ErrDatabaseClosed", err)
		}
		if db.file != nil {
			t.Error("closed database still holds a file")
		}
	}
	if !c.leaks {
		assertNoOpenFiles(t, dir)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	wantKeys(t, reopened, state)
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// C6: Shrink failure states. Without filter, the first fsync on the locked
// thread is the temp file's (Never policy, no earlier sync), the second is the
// directory's. Each row names the call it injects and the filter that counts it.

func TestRegressShrinkTempSyncFails(t *testing.T) {
	runShrinkCase(t, "TestRegressShrinkTempSyncFails", shrinkCase{
		inject: "fsync:error=EIO:when=1", events: "fsync",
		hits: []string{`^\d+ fsync\(\d+<.*/s\.db\.\d+\.tmp>\)`},
		err:  syscall.EIO, open: true,
	})
}

// With fd6, the 1st close is ReadFile's in the fixture; the 2nd is the temp file's.
func TestRegressShrinkTempCloseFails(t *testing.T) {
	runShrinkCase(t, "TestRegressShrinkTempCloseFails", shrinkCase{
		inject: "close:error=EIO:when=2", events: "close", filter: "fd6",
		hits: []string{`^\d+ close\(6<.*/s\.db\.\d+\.tmp>\)`},
		err:  syscall.EIO, open: true, leaks: true,
	})
}

// With db, the closes on s.db are ReadFile's, the aof copy's, and db.file's.
func TestRegressShrinkFileCloseFails(t *testing.T) {
	runShrinkCase(t, "TestRegressShrinkFileCloseFails", shrinkCase{
		inject: "close:error=EIO:when=3", events: "close", filter: "db",
		hits: []string{`^\d+ close\(\d+<.*/s\.db>\)`},
		err:  syscall.EIO, open: true, leaks: true,
	})
}

const renameFails = "renameat:error=EIO renameat2:error=EIO"

func TestRegressShrinkRenameFails(t *testing.T) {
	runShrinkCase(t, "TestRegressShrinkRenameFails", shrinkCase{
		inject: renameFails, events: "renameat,renameat2",
		hits: []string{`^\d+ renameat2?\(`},
		err:  syscall.EIO, open: true,
	})
}

// With db, the openat calls on s.db are Open's, ReadFile's, the aof copy's,
// and then the reopen. Only the reopen has no O_CREAT and is O_RDWR.
const reopenFails = "openat:error=EIO:when=4"
const reopenLine = `^\d+ openat\(.*/s\.db", O_RDWR(\|O_LARGEFILE)?\|O_CLOEXEC\)` // 386 adds O_LARGEFILE

func TestRegressShrinkReopenAfterRenameFails(t *testing.T) {
	runShrinkCase(t, "TestRegressShrinkReopenAfterRenameFails", shrinkCase{
		inject: reopenFails, events: "openat", filter: "db",
		hits: []string{reopenLine},
		err:  syscall.EIO, newFile: true,
	})
}

func TestRegressShrinkReopenOriginalFails(t *testing.T) {
	runShrinkCase(t, "TestRegressShrinkReopenOriginalFails", shrinkCase{
		// Different errnos show that Shrink reports both failures.
		inject: renameFails + " openat:error=EACCES:when=4", events: "openat,renameat,renameat2", filter: "db",
		hits: []string{`^\d+ renameat2?\(`, reopenLine},
		err:  syscall.EIO, alsoErr: syscall.EACCES,
	})
}

// With db, the lseek calls on s.db are Open's, Shrink's end-of-file, the aof
// copy's, and then the reopen's SEEK_END. The handle must be closed, not kept.
// On 386 the call is _llseek, which also passes a pointer for the new offset.
func TestRegressShrinkSeekFails(t *testing.T) {
	runShrinkCase(t, "TestRegressShrinkSeekFails", shrinkCase{
		inject: "lseek:error=EIO:when=4 _llseek:error=EIO:when=4", events: "lseek,_llseek", filter: "db",
		hits: []string{`^\d+ (_l)?lseek\(\d+<.*/s\.db>, 0, (\S+ )?SEEK_END\)`},
		err:  syscall.EIO, newFile: true,
	})
}

func TestRegressShrinkDirSyncFails(t *testing.T) {
	runShrinkCase(t, "TestRegressShrinkDirSyncFails", shrinkCase{
		inject: "fsync:error=EIO:when=2", events: "fsync",
		hits: []string{`^\d+ fsync\(\d+<.*/001>\)`},
		err:  syscall.EIO, newFile: true, open: true,
	})
}

// C6 reproducer: the temp file is synced before the rename, and the
// directory after it. On the evidence baseline the trace had no fsync at all
// between openat(s.db.tmp) and renameat.
func TestRegressShrinkSyncsBeforeRename(t *testing.T) {
	if os.Getenv("BUNTDB_SYNC_TEST_CHILD") == "1" {
		db, _, _ := shrinkFixture(t)
		if err := db.Shrink(); err != nil {
			t.Fatal(err)
		}
		return
	}
	dir := t.TempDir()
	t.Setenv("BUNTDB_SHRINK_DIR", dir)
	trace := runUnderStrace(t, "TestRegressShrinkSyncsBeforeRename", "", "fsync,renameat,renameat2")
	tmpSync := regexp.MustCompile(`fsync\(\d+<.*/s\.db\.\d+\.tmp>\) += 0`)
	rename := regexp.MustCompile(`renameat2?\(.*\) += 0`)
	dirSync := regexp.MustCompile(`fsync\(\d+<` + regexp.QuoteMeta(dir) + `>\) += 0`)
	at := func(re *regexp.Regexp) int {
		return slices.IndexFunc(joinStraceLines(trace), re.MatchString)
	}
	if ts, rn, ds := at(tmpSync), at(rename), at(dirSync); ts < 0 || rn < 0 || ds < 0 || !(ts < rn && rn < ds) {
		t.Fatalf("want temp fsync (%d) < rename (%d) < directory fsync (%d) in:\n%s", ts, rn, ds, trace)
	}
}

// Security tests: the new file must not be reachable through a planted path,
// and must not be more open than the old one.

// lookTool returns the path of a tool the test needs. CI requires it.
func lookTool(t *testing.T, name string) string {
	t.Helper()
	path, err := exec.LookPath(name)
	if err != nil {
		if os.Getenv("BUNTDB_REQUIRE_SYNC_TEST") == "1" {
			t.Fatalf("%s is required for the Linux Shrink permission check", name)
		}
		t.Skipf("requires %s", name)
	}
	return path
}

// setfacl runs setfacl, and skips the test if the file system has no ACLs.
func setfacl(t *testing.T, args ...string) {
	t.Helper()
	if out, err := exec.Command(lookTool(t, "setfacl"), args...).CombinedOutput(); err != nil {
		t.Skipf("setfacl %v: %v: %s", args, err, out)
	}
}

// readACL returns the access ACL of path, or nil if it has none.
func readACL(t *testing.T, path string) []byte {
	t.Helper()
	buf := make([]byte, 1024)
	n, err := syscall.Getxattr(path, accessACL, buf)
	if errors.Is(err, syscall.ENODATA) {
		return nil
	}
	if err != nil {
		t.Fatalf("getxattr %s: %v", path, err)
	}
	return buf[:n]
}

func shrinkOK(t *testing.T, db *DB) {
	t.Helper()
	if err := db.Shrink(); err != nil {
		t.Fatal(err)
	}
}

func TestRegressShrinkIgnoresTempSymlink(t *testing.T) {
	db, path, _ := shrinkFixture(t)
	sentinel := filepath.Join(t.TempDir(), "sentinel")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(sentinel, path+".tmp"); err != nil { // the old fixed temp path
		t.Fatal(err)
	}
	shrinkOK(t, db)
	if got, err := os.ReadFile(sentinel); err != nil || string(got) != "keep" {
		t.Errorf("sentinel = %q, %v; want it unchanged", got, err)
	}
	if fi, err := os.Lstat(path + ".tmp"); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Errorf("the planted symlink was replaced: %v, %v", fi, err)
	}
	wantKeys(t, db, map[string]string{"a": "2", "b": "3"})
}

func TestRegressShrinkKeepsPrivateMode(t *testing.T) {
	db, path, _ := shrinkFixture(t)
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	shrinkOK(t, db)
	if got := mustStat(t, path).Mode().Perm(); got != 0o600 {
		t.Fatalf("mode after Shrink = %v, want 0600", got)
	}
}

func TestRegressShrinkKeepsGroup(t *testing.T) {
	gid := -1
	groups, _ := os.Getgroups() // an error leaves no candidates
	for _, g := range groups {
		if g != os.Getegid() {
			gid = g
			break
		}
	}
	if gid < 0 && os.Geteuid() == 0 {
		gid = 65534
	}
	if gid < 0 {
		t.Skip("no group other than the primary group to set")
	}
	db, path, _ := shrinkFixture(t)
	if err := os.Chown(path, -1, gid); err != nil {
		t.Skipf("cannot set group %d: %v", gid, err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	shrinkOK(t, db)
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := int(fi.Sys().(*syscall.Stat_t).Gid); got != gid || fi.Mode().Perm() != 0o640 {
		t.Fatalf("after Shrink gid = %d, mode = %v; want %d, 0640", got, fi.Mode().Perm(), gid)
	}
}

// The directory's default ACL gives a named user read access to new files.
// The shrunk file must have the old file's ACL, not the directory's.
func TestRegressShrinkKeepsACL(t *testing.T) {
	const named = "65534" // a user other than the owner
	for _, tc := range []struct {
		name string
		old  []string // setfacl arguments for the old file; none: no ACL
	}{
		{"none", nil},
		{"named-user", []string{"-m", "u:" + named + ":rw"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, path, _ := shrinkFixture(t)
			if err := os.Chmod(path, 0o640); err != nil {
				t.Fatal(err)
			}
			if tc.old != nil {
				setfacl(t, append(tc.old, path)...)
			}
			setfacl(t, "-d", "-m", "u:"+named+":r,u:1:rw", filepath.Dir(path))
			want := readACL(t, path)
			wantMode := mustStat(t, path).Mode().Perm()
			shrinkOK(t, db)
			if got := readACL(t, path); !bytes.Equal(got, want) {
				t.Errorf("access ACL = %x, want the old file's %x", got, want)
			}
			if got := mustStat(t, path).Mode().Perm(); got != wantMode {
				t.Errorf("mode = %v, want %v", got, wantMode)
			}
		})
	}
}

func mustStat(t *testing.T, path string) os.FileInfo {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi
}

// posixACL encodes a version 2 POSIX ACL xattr: rw for the owner, r for the
// group and mask, none for others, and r for user 65534.
func posixACL() []byte {
	b := binary.LittleEndian.AppendUint32(nil, 2)
	for _, e := range []struct {
		tag, perm uint16
		id        uint32
	}{{0x01, 6, ^uint32(0)}, {0x02, 4, 65534}, {0x04, 4, ^uint32(0)}, {0x10, 4, ^uint32(0)}, {0x20, 0, ^uint32(0)}} {
		b = binary.LittleEndian.AppendUint16(b, e.tag)
		b = binary.LittleEndian.AppendUint16(b, e.perm)
		b = binary.LittleEndian.AppendUint32(b, e.id)
	}
	return b
}

// aclCase is a Shrink on a file with a POSIX ACL, with one injected failure
// in the ACL or owner step. Shrink must succeed and narrow the new file.
type aclCase struct {
	inject, events string
	hit            string // regexp for the injected line
	dirDefault     bool   // the directory has a default ACL that new files inherit
	narrowed       bool   // the new file has mode 0600 & old, and no ACL; else the old mode
}

// runACLCase runs one aclCase in a strace subprocess. The child sets the ACLs
// with setxattr, not setfacl, so that the injection cannot reach another
// process, and its own xattr calls are the only ones before Shrink's.
func runACLCase(t *testing.T, test string, c aclCase) {
	if os.Getenv("BUNTDB_SYNC_TEST_CHILD") != "1" {
		dir := t.TempDir()
		t.Setenv("BUNTDB_SHRINK_DIR", dir)
		probe := filepath.Join(dir, "probe")
		if err := os.WriteFile(probe, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Setxattr(probe, accessACL, posixACL(), 0); err != nil {
			t.Skipf("the file system does not support ACLs: %v", err)
		}
		if err := os.Remove(probe); err != nil {
			t.Fatal(err)
		}
		trace := runUnderStrace(t, test, c.inject, c.events)
		var injected []string
		for _, line := range joinStraceLines(trace) {
			if strings.Contains(line, "(INJECTED)") {
				injected = append(injected, line)
			}
		}
		if len(injected) != 1 || !regexp.MustCompile(c.hit).MatchString(injected[0]) {
			t.Fatalf("injected %q, want one call matching %q:\n%s", injected, c.hit, trace)
		}
		return
	}
	runtime.LockOSThread() // strace counts per thread: keep the child's and Shrink's calls together
	defer runtime.UnlockOSThread()
	db, path, _ := shrinkFixture(t)
	if err := syscall.Setxattr(path, accessACL, posixACL(), 0); err != nil {
		t.Fatal(err)
	}
	if c.dirDefault {
		if err := syscall.Setxattr(filepath.Dir(path), "system.posix_acl_default", posixACL(), 0); err != nil {
			t.Fatal(err)
		}
	}
	oldMode := mustStat(t, path).Mode().Perm()
	shrinkOK(t, db)
	want := oldMode
	if c.narrowed {
		want &= 0o600
	}
	if got := mustStat(t, path).Mode().Perm(); got != want {
		t.Errorf("mode = %v, want %v", got, want)
	}
	if acl := readACL(t, path); c.narrowed && acl != nil {
		t.Errorf("access ACL = %x, want none", acl)
	}
	wantKeys(t, db, map[string]string{"a": "2", "b": "3"})
}

// When the owner cannot be copied, the new file stays 0600 & the old mode and
// has no ACL, although the old file has one and the directory has a default
// ACL. The child makes fchown fail (386 has fchown32).
func TestRegressShrinkChownFailureNarrows(t *testing.T) {
	runACLCase(t, "TestRegressShrinkChownFailureNarrows", aclCase{
		inject: "fchown:error=EPERM fchown32:error=EPERM", events: "fchown,fchown32",
		hit: `fchown(32)?\(`, dirDefault: true, narrowed: true,
	})
}

// The first getxattr in the child is Shrink's read of the old file's ACL.
func TestRegressShrinkACLReadFailureNarrows(t *testing.T) {
	runACLCase(t, "TestRegressShrinkACLReadFailureNarrows", aclCase{
		inject: "getxattr:error=EIO:when=1", events: "getxattr",
		hit: `getxattr\("/proc/self/fd/\d+", "system.posix_acl_access"`, dirDefault: true, narrowed: true,
	})
}

// ENOTSUP means that the file system has no ACLs, so Shrink keeps the mode.
// The directory has no default ACL, because the file system would have none.
func TestRegressShrinkACLReadNotSupported(t *testing.T) {
	runACLCase(t, "TestRegressShrinkACLReadNotSupported", aclCase{
		inject: "getxattr:error=EOPNOTSUPP:when=1", events: "getxattr",
		hit: `getxattr\("/proc/self/fd/\d+", "system.posix_acl_access"`,
	})
}

// The child's own setxattr on the old file is the first; Shrink's is the second.
func TestRegressShrinkACLWriteNotSupported(t *testing.T) {
	runACLCase(t, "TestRegressShrinkACLWriteNotSupported", aclCase{
		inject: "setxattr:error=EOPNOTSUPP:when=2", events: "setxattr",
		hit: `setxattr\("/proc/self/fd/\d+", "system.posix_acl_access"`,
	})
}

// If Shrink cannot remove an ACL that the new file may carry, it fails before
// the swap. The old file stays in use.
func TestRegressShrinkACLRemoveFailureAborts(t *testing.T) {
	runShrinkCase(t, "TestRegressShrinkACLRemoveFailureAborts", shrinkCase{
		inject: "getxattr:error=EIO:when=1 removexattr:error=EACCES", events: "getxattr,removexattr",
		hits: []string{`getxattr\(`, `removexattr\(`},
		err:  syscall.EACCES, open: true,
	})
}
