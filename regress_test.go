package buntdb

// Regression coverage for the fixes in this series. TestControl* pin behaviour
// that already worked and must keep working; TestRegress* reproduce a defect and
// fail on the original code.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

// A7: CreateSpatialIndexOptions accepted an *IndexOptions and passed nil to
// createIndex, so CaseInsensitiveKeyMatching never took effect on a spatial
// index. Its sibling CreateIndexOptions passes opts correctly; commit 27d3577
// introduced both with that asymmetry.
//
// The b-tree half of this test is the control: it shows the option works there,
// so a failure is specific to the spatial path rather than to the option.
func TestRegressSpatialIndexOptionsHonoured(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		if err := tx.CreateSpatialIndexOptions("spatial", "POINT:*",
			&IndexOptions{CaseInsensitiveKeyMatching: true}, IndexRect); err != nil {
			return err
		}
		return tx.CreateIndexOptions("btree", "POINT:*",
			&IndexOptions{CaseInsensitiveKeyMatching: true}, IndexString)
	}); err != nil {
		t.Fatal(err)
	}

	// Key cased differently from the pattern. With the option honoured, both
	// indexes must pick it up.
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("point:1", Point(10, 10), nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	var spatial, btree int
	if err := db.View(func(tx *Tx) error {
		if err := tx.Intersects("spatial", Rect([]float64{0, 0}, []float64{20, 20}),
			func(k, v string) bool { spatial++; return true }); err != nil {
			return err
		}
		return tx.Ascend("btree", func(k, v string) bool { btree++; return true })
	}); err != nil {
		t.Fatal(err)
	}

	if btree != 1 {
		t.Fatalf("control failed: case-insensitive b-tree index holds %d items, want 1", btree)
	}
	if spatial != 1 {
		t.Errorf("case-insensitive spatial index holds %d items, want 1: "+
			"CaseInsensitiveKeyMatching is being discarded", spatial)
	}
}

// A3: readLoad indexed parts[0][1] and parts[0][2] after checking only
// len(parts) == 0, so a command name shorter than three characters panicked
// inside Open. Commit f240a9c guarded this with a length check; b559e28
// removed it to admit the seven-character "flushdb".
func TestRegressShortCommandName(t *testing.T) {
	for _, tc := range []struct {
		name string
		aof  string
	}{
		{"empty name", "*1\r\n$0\r\n\r\n"},
		{"one char", "*1\r\n$1\r\ns\r\n"},
		{"two chars", "*1\r\n$2\r\nse\r\n"},
		{"prefix only", "*3\r\n$4\r\nsetx\r\n$1\r\na\r\n$1\r\n1\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.db")
			if err := os.WriteFile(path, []byte(tc.aof), 0666); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Open panicked on a malformed command name: %v", r)
				}
			}()
			db, err := Open(path)
			if db != nil {
				defer db.Close()
			}
			if err != ErrInvalid {
				t.Fatalf("Open = %v, want ErrInvalid", err)
			}
		})
	}
}

// Command names are matched case-insensitively. This guards the A3 length-check
// restoration against accidentally making the parser case-sensitive.
func TestControlCommandNameCase(t *testing.T) {
	for _, tc := range []struct {
		name string
		aof  string
		key  string
		want string // "" means the key must be absent
	}{
		{"set lower", "*3\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\n1\r\n", "a", "1"},
		{"set upper", "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n", "a", "1"},
		{"set mixed", "*3\r\n$3\r\nSeT\r\n$1\r\na\r\n$1\r\n1\r\n", "a", "1"},
		{"del lower", "*3\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$3\r\ndel\r\n$1\r\na\r\n", "a", ""},
		{"del upper", "*3\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$3\r\nDEL\r\n$1\r\na\r\n", "a", ""},
		{"flushdb lower", "*3\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\n1\r\n*1\r\n$7\r\nflushdb\r\n", "a", ""},
		{"flushdb upper", "*3\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\n1\r\n*1\r\n$7\r\nFLUSHDB\r\n", "a", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "data.db")
			if err := os.WriteFile(path, []byte(tc.aof), 0666); err != nil {
				t.Fatal(err)
			}
			db, err := Open(path)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer db.Close()
			if err := db.View(func(tx *Tx) error {
				v, err := tx.Get(tc.key)
				if tc.want == "" {
					if err != ErrNotFound {
						return fmt.Errorf("key %q = %q (%v), want ErrNotFound", tc.key, v, err)
					}
					return nil
				}
				if err != nil {
					return fmt.Errorf("key %q: %w", tc.key, err)
				}
				if v != tc.want {
					return fmt.Errorf("key %q = %q, want %q", tc.key, v, tc.want)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// RESP command names are ASCII; Unicode case folding must not turn malformed
// input into a command, particularly a destructive FLUSHDB.
func TestControlASCIICommandNames(t *testing.T) {
	for _, command := range []string{"ſet", "fluſhdb"} {
		t.Run(command, func(t *testing.T) {
			db, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			aof := fmt.Sprintf("*3\r\n$%d\r\n%s\r\n$1\r\na\r\n$1\r\nb\r\n", len(command), command)
			if command == "fluſhdb" {
				aof = fmt.Sprintf("*1\r\n$%d\r\n%s\r\n", len(command), command)
			}
			if err := db.Load(strings.NewReader(aof)); err != ErrInvalid {
				t.Fatalf("Load(%q) = %v, want ErrInvalid", command, err)
			}
		})
	}
}

// newLoadTarget builds the same in-memory DB state that Open(":memory:")
// produces, without starting the background goroutine. readLoad needs the trees
// initialised; it does not need a file or a background manager.
func newLoadTarget() *DB {
	db := &DB{}
	db.keys = btreeNew(lessCtx(nil))
	db.exps = btreeNew(lessCtx(&exctx{db}))
	db.idxs = make(map[string]*index)
	return db
}

// appendSet encodes one valid SET record using the production writer, so a
// corpus built here is exactly what buntdb itself would have written.
func appendSet(buf []byte, key, val string, now time.Time) []byte {
	return (&dbItem{key: key, val: val}).writeSetTo(buf, now)
}

// A truncated final command is explicitly allowed: load() truncates the file to
// the end of the last valid command (buntdb.go, "which is allowed"). This
// guards the A2 parser change against breaking recovery, which has named
// production users (upstream issues #72, #82).
func TestControlIncompleteTailRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.db")
	now := time.Now()

	var buf []byte
	for i := range 3 {
		buf = appendSet(buf, fmt.Sprintf("key:%d", i), fmt.Sprintf("val:%d", i), now)
	}
	validLen := len(buf)

	// Append a command cut in half.
	partial := appendSet(nil, "partial", "should-not-load", now)
	buf = append(buf, partial[:len(partial)/2]...)

	if err := os.WriteFile(path, buf, 0666); err != nil {
		t.Fatal(err)
	}

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open must recover from an incomplete tail, got: %v", err)
	}
	// Bind cleanup to this specific handle, not the variable reassigned on
	// reopen below. A t.Fatal before the explicit Close would otherwise leave
	// the file and background manager open, which blocks TempDir removal on
	// Windows. Close is idempotent: a second call returns ErrDatabaseClosed.
	first := db
	t.Cleanup(func() { _ = first.Close() })

	// The three complete records survived; the half-written one did not.
	if err := db.View(func(tx *Tx) error {
		for i := range 3 {
			v, err := tx.Get(fmt.Sprintf("key:%d", i))
			if err != nil {
				return fmt.Errorf("key:%d missing after recovery: %w", i, err)
			}
			if want := fmt.Sprintf("val:%d", i); v != want {
				return fmt.Errorf("key:%d = %q, want %q", i, v, want)
			}
		}
		if _, err := tx.Get("partial"); err != ErrNotFound {
			return fmt.Errorf("partial record loaded, want ErrNotFound, got %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// The file was truncated back to the end of the last valid command.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != int64(validLen) {
		t.Fatalf("file size after recovery = %d, want %d (end of last valid command)",
			fi.Size(), validLen)
	}

	// A write after recovery lands at the recovered position and survives.
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("after-recovery", "ok", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen after recovery: %v", err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		if v, err := tx.Get("after-recovery"); err != nil || v != "ok" {
			return fmt.Errorf("post-recovery write lost: %q, %v", v, err)
		}
		if _, err := tx.Get("key:0"); err != nil {
			return fmt.Errorf("pre-recovery data lost: %w", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// A legitimately large record must round-trip. This is the control that stops
// the A2 allocation fix from introducing an arbitrary record-size ceiling.
func TestControlLargeRecordRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.db")

	large := strings.Repeat("x", 1<<20) // 1 MiB
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	first := db // see the note in TestControlIncompleteTailRecovery
	t.Cleanup(func() { _ = first.Close() })
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("big", large, nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen with a 1 MiB record: %v", err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		v, err := tx.Get("big")
		if err != nil {
			return err
		}
		if len(v) != len(large) || v != large {
			return fmt.Errorf("large value corrupted: got %d bytes, want %d", len(v), len(large))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Expired and non-expiring records both survive a save/load round trip through
// readLoad. Guards the A2/A3 changes against mishandling the 5-part SET form.
func TestControlExpiryRoundTrip(t *testing.T) {
	now := time.Now()
	var buf []byte
	buf = appendSet(buf, "plain", "value", now)
	buf = (&dbItem{
		key:  "living",
		val:  "value",
		opts: &dbItemOpts{ex: true, exat: now.Add(time.Hour)},
	}).writeSetTo(buf, now)
	buf = (&dbItem{
		key:  "dead",
		val:  "value",
		opts: &dbItemOpts{ex: true, exat: now.Add(-time.Hour)},
	}).writeSetTo(buf, now)

	db := newLoadTarget()
	if _, err := db.readLoad(bytes.NewReader(buf), now); err != nil {
		t.Fatalf("readLoad: %v", err)
	}
	if db.get("plain") == nil {
		t.Error("non-expiring record lost")
	}
	if db.get("living") == nil {
		t.Error("unexpired record lost")
	}
	if db.get("dead") != nil {
		t.Error("already-expired record was loaded, want it dropped")
	}
}

// Headers may exceed bufio's buffer (for example, with leading zeroes). The
// allocation-free header path must preserve their parsing and EOF behavior.
func TestControlLoadLongHeaders(t *testing.T) {
	type loadCase struct {
		name, aof string
		wantErr   error
	}
	cases := []loadCase{
		{"truncated array", "*" + strings.Repeat("0", 8192), io.ErrUnexpectedEOF},
		{"truncated bulk", "*1\r\n$" + strings.Repeat("0", 8192), io.ErrUnexpectedEOF},
		{"invalid array", "*" + strings.Repeat("0", 8192) + "x\r\n", ErrInvalid},
		{"invalid bulk", "*1\r\n$" + strings.Repeat("0", 8192) + "x\r\n", ErrInvalid},
	}
	for _, size := range []int{4095, 4096, 4097, 12288} {
		digits := strings.Repeat("0", size-4) + "3\r\n"
		cases = append(cases,
			loadCase{fmt.Sprintf("array/%d", size), "*" + digits + "$3\r\nset\r\n$1\r\nk\r\n$1\r\nv\r\n", nil},
			loadCase{fmt.Sprintf("bulk/%d", size), "*3\r\n$" + digits + "set\r\n$1\r\nk\r\n$1\r\nv\r\n", nil})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.Load(iotest.OneByteReader(strings.NewReader(tc.aof))); err != tc.wantErr {
				t.Fatalf("Load = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil {
				if err := db.View(func(tx *Tx) error {
					if value, err := tx.Get("k"); err != nil || value != "v" {
						return fmt.Errorf("loaded value = %q, %v; want v", value, err)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// A2a: the declared bulk length was parsed with unchecked arithmetic, so a
// value beyond the int range wrapped negative and then sliced out of bounds.
func TestRegressLengthOverflow(t *testing.T) {
	for _, tc := range []struct {
		name string
		aof  string
	}{
		{"bulk length overflow", "*3\r\n$3\r\nset\r\n$18446744073709551615\r\nx\r\n$1\r\ny\r\n"},
		{"array count overflow", "*99999999999999999999\r\n$3\r\nset\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.db")
			if err := os.WriteFile(path, []byte(tc.aof), 0666); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Open panicked on an overflowing length: %v", r)
				}
			}()
			db, err := Open(path)
			if db != nil {
				defer db.Close()
			}
			if err != ErrInvalid {
				t.Fatalf("Open = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestRegressLengthBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name string
		aof  string
		want error
	}{
		{"max array count", "*" + strconv.Itoa(math.MaxInt) + "\r\n", io.ErrUnexpectedEOF},
		{"max bulk with terminator", "*1\r\n$" + strconv.Itoa(math.MaxInt-2) + "\r\n", io.ErrUnexpectedEOF},
		{"terminator overflow by one", "*1\r\n$" + strconv.Itoa(math.MaxInt-1) + "\r\n", ErrInvalid},
		{"terminator overflow by two", "*1\r\n$" + strconv.Itoa(math.MaxInt) + "\r\n", ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.Load(strings.NewReader(tc.aof)); err != tc.want {
				t.Fatalf("Load = %v, want %v", err, tc.want)
			}
		})
	}
}

// A2b: the read buffer was sized from the declared length before any of the
// body arrived, so a tiny corrupt header could force a huge allocation. A
// 29-byte file declaring 1 GiB allocated 2 GiB. Memory must instead be
// proportional to the bytes actually read.
func TestRegressAllocationBoundedByBytesRead(t *testing.T) {
	// A 16 MiB declaration still detects eager allocation, but limits even the
	// old parser to 32 MiB rather than risking a 2 GiB allocation in the suite.
	tail := "*3\r\n$3\r\nset\r\n$16777216\r\nx\r\n"
	prefix := string(appendSet(nil, "kept", "value", time.Now()))
	aof := prefix + tail
	memory, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer memory.Close()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err = memory.Load(strings.NewReader(aof))
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("Load = %v, want io.ErrUnexpectedEOF", err)
	}
	t.Logf("%d-byte input allocated %d bytes", len(aof), allocated)
	if allocated > 8<<20 {
		t.Errorf("%d-byte input allocated %d bytes; want at most 8 MiB", len(aof), allocated)
	}

	path := filepath.Join(t.TempDir(), "data.db")
	if err := os.WriteFile(path, []byte(aof), 0666); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open must recover the incomplete tail: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	db.mu.Lock()
	pos, seekErr := db.file.Seek(0, io.SeekCurrent)
	db.mu.Unlock()
	if seekErr != nil || pos != int64(len(prefix)) {
		t.Fatalf("recovered position = %d, %v; want %d", pos, seekErr, len(prefix))
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(len(prefix)) {
		t.Fatalf("recovered file size = %d, want %d", info.Size(), len(prefix))
	}
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("after", "write", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.View(func(tx *Tx) error {
		for key, want := range map[string]string{"kept": "value", "after": "write"} {
			if got, err := tx.Get(key); err != nil || got != want {
				return fmt.Errorf("reopened %q = %q, %v; want %q", key, got, err, want)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// FuzzReadLoad exercises the AOF parser against arbitrary input. readLoad is
// reached from Open on any existing database file, so its inputs are untrusted
// whenever a file is corrupt or hand-written.
//
// The oracle checks for panics and out-of-range recovery offsets. Allocation
// is checked in the focused regression above: process-wide MemStats also count
// unrelated goroutines and fuzzing machinery, so are not a per-input oracle.
func FuzzReadLoad(f *testing.F) {
	now := time.Now()

	// Valid commands, taken from the shapes TestDatabaseFormat covers.
	f.Add([]byte("*3\r\n$3\r\nset\r\n$4\r\nvar1\r\n$4\r\n1234\r\n"))
	f.Add([]byte("*2\r\n$3\r\ndel\r\n$4\r\nvar1\r\n"))
	f.Add([]byte("*1\r\n$7\r\nflushdb\r\n"))
	f.Add([]byte("*5\r\n$3\r\nset\r\n$3\r\nvar\r\n$3\r\nval\r\n$2\r\nex\r\n$2\r\n10\r\n"))
	f.Add([]byte("*5\r\n$3\r\nset\r\n$3\r\nvar\r\n$3\r\nval\r\n$2\r\nae\r\n$2\r\n10\r\n"))
	f.Add(append([]byte{0, 0, 0}, "*2\r\n$3\r\nDEL\r\n$5\r\nHELLO\r\n"...)) // NUL padding
	// Malformed shapes, including the two defects this step fixes.
	f.Add([]byte("*1\r\n$1\r\ns\r\n"))
	f.Add([]byte("*1\r\n$0\r\n\r\n"))
	f.Add([]byte("*3\r\n$3\r\nset\r\n$18446744073709551615\r\nx\r\n$1\r\ny\r\n"))
	f.Add([]byte("*3\r\n$3\r\nset\r\n$16777216\r\nx\r\n"))
	f.Add([]byte("*3\r\n$3\r\nset\r\n$4\r\nvar1\r\n$4\r\n12")) // incomplete tail

	f.Add([]byte("*1\r\n$8\r\nfluſhdb\r\n"))
	f.Add([]byte("*1\r\n$" + strconv.Itoa(math.MaxInt-2) + "\r\n"))
	// Leading zeroes can make valid headers span several reader buffers.
	f.Add([]byte("*" + strings.Repeat("0", 8192) + "1\r\n$7\r\nflushdb\r\n"))
	f.Add([]byte("*1\r\n$" + strings.Repeat("0", 8192) + "7\r\nflushdb\r\n"))

	f.Fuzz(func(t *testing.T, in []byte) {
		db := newLoadTarget()
		n, err := db.readLoad(bytes.NewReader(in), now)

		// The reported offset is used by load() to truncate, so it must never
		// exceed the input it was derived from.
		if n < 0 || n > int64(len(in)) {
			t.Fatalf("readLoad returned offset %d for a %d-byte input (err=%v)", n, len(in), err)
		}
	})
}

func benchmarkLoad(b *testing.B, corpus []byte) {
	b.SetBytes(int64(len(corpus)))
	b.ReportAllocs()
	now := time.Now()
	b.ResetTimer()
	for b.Loop() {
		db := newLoadTarget()
		if _, err := db.readLoad(bytes.NewReader(corpus), now); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Load_SmallRecords(b *testing.B) {
	now := time.Now()
	var corpus []byte
	for i := range 20000 {
		corpus = appendSet(corpus,
			fmt.Sprintf("user:%012d", i),
			fmt.Sprintf("value-%012d", i), now)
	}
	benchmarkLoad(b, corpus)
}

func Benchmark_Load_LargeValues(b *testing.B) {
	now := time.Now()
	large := strings.Repeat("x", 1<<20) // 1 MiB
	var corpus []byte
	for i := range 8 {
		corpus = appendSet(corpus, fmt.Sprintf("big:%d", i), large, now)
	}
	benchmarkLoad(b, corpus)
}

// A5: Shrink takes db.mu, sets db.shrinking, and registers a deferred cleanup
// that re-acquires db.mu. If the initial Seek failed, the function returned
// while still holding the lock, so that cleanup deadlocked against itself.
//
// Public-API reachability has not been demonstrated. This test closes the
// private file to force the error; prior successful I/O does not prove later
// I/O cannot fail.
func TestRegressShrinkReleasesLockOnEarlyFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("k", "v", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// Inject: make Shrink's first Seek fail.
	if err := db.file.Close(); err != nil {
		t.Fatal(err)
	}

	shrunk := make(chan error, 1)
	go func() { shrunk <- db.Shrink() }()
	select {
	case err := <-shrunk:
		if !errors.Is(err, os.ErrClosed) {
			t.Fatalf("Shrink error = %v, want the closed-file seek error", err)
		}
		t.Logf("Shrink reported the seek failure: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Shrink deadlocked against its own deferred cleanup")
	}

	// A retry must reach Seek again, not return ErrShrinkInProcess because
	// deferred cleanup forgot to reset db.shrinking. Also verify reads work.
	// Keep both checks timed so a lock regression reports rather than hangs.
	released := make(chan error, 1)
	go func() {
		if err := db.Shrink(); !errors.Is(err, os.ErrClosed) {
			released <- fmt.Errorf("Shrink retry = %v, want the closed-file seek error", err)
			return
		}
		released <- db.View(func(tx *Tx) error {
			if value, err := tx.Get("k"); err != nil || value != "v" {
				return fmt.Errorf("value after failed Shrink = %q, %v; want v", value, err)
			}
			return nil
		})
	}()
	select {
	case err := <-released:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("db.mu was still held after Shrink returned")
	}

	// Only safe to close once the lock is known to be free; the file is
	// already closed, so the error here is expected and not the subject.
	_ = db.Close()
}

// A1: managed() left err nil when the callback did not return normally, so its
// deferred cleanup took the commit branch. Writes made before a panic were
// committed, and the same held for runtime.Goexit -- which is why the fix uses
// a normal-completion flag rather than recover(), since recover() never sees a
// Goexit.
//
// The godoc did not previously say what happens on abnormal exit, so this
// establishes the contract rather than restoring one.
func TestRegressAbnormalExitDoesNotCommit(t *testing.T) {
	for _, tc := range []struct {
		name      string
		exit      func()
		wantPanic any
	}{
		{"panic", func() { panic("callback failed") }, "callback failed"},
		{"goexit", runtime.Goexit, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}

			// A committed value proves rollback is scoped to this transaction.
			if err := db.Update(func(tx *Tx) error {
				_, _, err := tx.Set("before", "kept", nil)
				return err
			}); err != nil {
				db.Close()
				t.Fatal(err)
			}

			var panicValue any
			var returned bool
			var updateErr, checkErr error
			var aborted *Tx
			done := make(chan struct{})
			go func() {
				defer close(done)
				// Keep validation and Close inside the timed goroutine: either
				// can block if transaction cleanup failed to release its lock.
				defer db.Close()
				defer func() {
					panicValue = recover()
					checkErr = db.View(func(tx *Tx) error {
						if _, err := tx.Get("during"); err != ErrNotFound {
							return fmt.Errorf(`"during" survived an aborted transaction (err=%v)`, err)
						}
						if v, err := tx.Get("before"); err != nil || v != "kept" {
							return fmt.Errorf(`"before" lost: %q %v`, v, err)
						}
						return nil
					})
				}()
				updateErr = db.Update(func(tx *Tx) error {
					aborted = tx
					if _, _, err := tx.Set("during", "must-not-survive", nil); err != nil {
						return err
					}
					tc.exit()
					return nil
				})
				returned = true
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("abnormal transaction cleanup, validation, or Close blocked")
			}

			if returned {
				t.Errorf("Update returned normally after %s (err=%v)", tc.name, updateErr)
			}
			if panicValue != tc.wantPanic {
				t.Errorf("panic = %v, want %v", panicValue, tc.wantPanic)
			}
			if checkErr != nil {
				t.Error(checkErr)
			}
			if aborted == nil {
				t.Fatal("callback was not invoked")
			}
			if _, err := aborted.Get("during"); err != ErrTxClosed {
				t.Errorf("aborted transaction Get = %v, want ErrTxClosed", err)
			}
		})
	}
}
