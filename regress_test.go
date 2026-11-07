package buntdb

// Regression coverage for the fixes in this series. TestControl* pin behaviour
// that already worked and must keep working; TestRegress* reproduce a defect and
// fail on the original code.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
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
func appendSet(buf []byte, key, val string) []byte {
	return (&dbItem{key: key, val: val}).writeSetTo(buf)
}

// A truncated final command is explicitly allowed: load() truncates the file to
// the end of the last valid command (buntdb.go, "which is allowed"). This
// guards the A2 parser change against breaking recovery, which has named
// production users (upstream issues #72, #82).
func TestControlIncompleteTailRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.db")

	var buf []byte
	for i := range 3 {
		buf = appendSet(buf, fmt.Sprintf("key:%d", i), fmt.Sprintf("val:%d", i))
	}
	validLen := len(buf)

	// Append a command cut in half.
	partial := appendSet(nil, "partial", "should-not-load")
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
	buf = appendSet(buf, "plain", "value")
	buf = (&dbItem{
		key:  "living",
		val:  "value",
		opts: &dbItemOpts{ex: true, exat: now.Add(time.Hour)},
	}).writeSetTo(buf)
	buf = (&dbItem{
		key:  "dead",
		val:  "value",
		opts: &dbItemOpts{ex: true, exat: now.Add(-time.Hour)},
	}).writeSetTo(buf)

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

// Upstream issue #125: each crafted input must make Load return ErrInvalid,
// never panic, and never succeed silently.
func TestRegressIssue125(t *testing.T) {
	for _, tc := range []struct {
		name string
		aof  string
	}{
		{"1 one-byte SET name", "*3\r\n$1\r\nS\r\n$1\r\nk\r\n$1\r\nv\r\n"},
		{"2 two-byte DEL name", "*2\r\n$2\r\nDE\r\n$1\r\nk\r\n"},
		{"3 empty command name", "*1\r\n$0\r\n\r\n"},
		{"4 part size 2^63", "*3\r\n$3\r\nset\r\n$9223372036854775808\r\n"},
		{"5 command count 2^63", "*9223372036854775808\r\n$3\r\nset\r\n$1\r\nk\r\n$1\r\nv\r\n"},
		{"5b count 2^63 alone", "*9223372036854775808\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.Load(strings.NewReader(tc.aof)); err != ErrInvalid {
				t.Fatalf("Load = %v, want ErrInvalid", err)
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
	prefix := string(appendSet(nil, "kept", "value"))
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

// C1/C2: readLoad left NUL bytes before a command, and the bytes of "*0\r\n",
// out of the offset it returned. Open seeks to that offset, so the next commit
// overwrote the end of the preceding record.
func TestRegressLoadOffsetCountsSkippedBytes(t *testing.T) {
	set := func(k, v string) string { return string(appendSet(nil, k, v)) }
	for _, tc := range []struct {
		name, content string
		want          map[string]string
	}{
		{"nul between commands", set("a", "1") + "\x00\x00\x00\x00" + set("b", "2"),
			map[string]string{"a": "1", "b": "2", "c": "3"}},
		{"empty array", "*0\r\n" + set("b", "2"),
			map[string]string{"b": "2", "c": "3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "x.db")
			if err := os.WriteFile(path, []byte(tc.content), 0666); err != nil {
				t.Fatal(err)
			}
			db, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close() // a second Close after the explicit one is harmless
			if err := db.Update(func(tx *Tx) error {
				_, _, err := tx.Set("c", "3", nil)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = Open(path)
			if err != nil {
				raw, _ := os.ReadFile(path)
				t.Fatalf("reopen after append: %v; file=%q", err, raw)
			}
			defer db.Close()
			if got := dbContents(t, db); !maps.Equal(got, tc.want) {
				t.Fatalf("after reopen got %v, want %v", got, tc.want)
			}
		})
	}
}

// C1: truncation recovery cut a valid record one byte short when NUL bytes
// preceded it, so the record was lost on the next open.
func TestRegressNULThenTruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.db")
	valid := string(appendSet(nil, "k", "v"))
	if err := os.WriteFile(path, []byte("\x00"+valid+"*3\r\n$1\r\nS\r\n$1"), 0666); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() // a second Close after the explicit one is harmless
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "\x00"+valid {
		t.Errorf("after recovery file=%q, want NUL+valid (len %d)", raw, 1+len(valid))
	}
	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	if got, want := dbContents(t, db), map[string]string{"k": "v"}; !maps.Equal(got, want) {
		t.Errorf("after recovery got %v, want %v", got, want)
	}
}

// dbContents returns every key and value in db.
func dbContents(t *testing.T, db *DB) map[string]string {
	t.Helper()
	got := map[string]string{}
	if err := db.View(func(tx *Tx) error {
		return tx.Ascend("", func(k, v string) bool {
			got[k] = v
			return true
		})
	}); err != nil {
		t.Fatal(err)
	}
	return got
}

// The offset is where the caller truncates, so trailing NULs must not count.
func TestControlLoadOffsetExcludesTrailingNULs(t *testing.T) {
	cmd := string(appendSet(nil, "k", "v"))
	for _, tc := range []struct {
		name, in string
		want     int
	}{
		{"nul only", "\x00\x00\x00", 0},
		{"command then nuls", cmd + "\x00\x00\x00", len(cmd)},
		{"nul, command, nuls", "\x00" + cmd + "\x00\x00", 1 + len(cmd)},
		{"empty array then nuls", "*0\r\n\x00", 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, err := newLoadTarget().readLoad(strings.NewReader(tc.in), time.Now())
			if err != nil || n != int64(tc.want) {
				t.Fatalf("readLoad = (%d, %v), want (%d, nil)", n, err, tc.want)
			}
		})
	}
}

// loadedKeys lists the keys in db's b-tree, in order.
func loadedKeys(db *DB) []string {
	var keys []string
	db.keys.Walk(func(items []interface{}) {
		for _, v := range items {
			keys = append(keys, v.(*dbItem).key)
		}
	})
	return keys
}

// hasTTLSet reports whether b, a run of complete commands and NULs, holds a
// five-part SET, the only command with a TTL. Skipping bulk bodies keeps their
// bytes from reading as headers.
func hasTTLSet(b []byte) bool {
	// header consumes a "<c><digits>\r\n" line from the front of b.
	header := func(c byte) (int, bool) {
		end := bytes.Index(b, []byte("\r\n"))
		if end < 0 || b[0] != c {
			return 0, false
		}
		n, ok := parseRESPLen(b[:end+2])
		b = b[end+2:]
		return n, ok
	}
	for len(b) > 0 {
		if b[0] == 0 {
			b = b[1:]
			continue
		}
		count, ok := header('*')
		if !ok {
			return false
		}
		for i := 0; i < count; i++ {
			size, ok := header('$')
			if !ok || len(b) < size+2 {
				return false
			}
			if i == 0 && count == 5 && size == 3 && strings.EqualFold(string(b[:size]), "set") {
				return true
			}
			b = b[size+2:]
		}
	}
	return false
}

// FuzzReadLoad exercises the AOF parser against arbitrary input. readLoad is
// reached from Open on any existing database file, so its inputs are untrusted
// whenever a file is corrupt or hand-written.
//
// The oracle checks for panics, recovery offsets and the NUL tail. Allocation
// is checked in the focused regression above: process-wide MemStats also count
// unrelated goroutines and fuzzing machinery, so are not a per-input oracle.
func FuzzReadLoad(f *testing.F) {
	now := time.Now()

	// Valid commands, taken from the shapes TestDatabaseFormat covers.
	f.Add([]byte("*3\r\n$3\r\nset\r\n$4\r\nvar1\r\n$4\r\n1234\r\n"))
	f.Add([]byte("*2\r\n$3\r\ndel\r\n$4\r\nvar1\r\n"))
	f.Add([]byte("*1\r\n$7\r\nflushdb\r\n"))
	// NUL padding counts when a command follows, "*0" is a command, trailing NULs do not count.
	f.Add([]byte("\x00\x00*3\r\n$3\r\nset\r\n$1\r\nk\r\n$1\r\nv\r\n"))
	f.Add([]byte("*2\r\n$3\r\ndel\r\n$1\r\nk\r\n\x00\x00*1\r\n$7\r\nflushdb\r\n"))
	f.Add([]byte("*0\r\n*2\r\n$3\r\ndel\r\n$1\r\nk\r\n"))
	f.Add([]byte("*1\r\n$7\r\nflushdb\r\n\x00\x00\x00"))                              // trailing NULs
	f.Add([]byte("\x00*3\r\n$3\r\nset\r\n$1\r\nk\r\n$1\r\nv\r\n*3\r\n$1\r\nS\r\n$1")) // NUL, then a truncated tail
	f.Add([]byte("*5\r\n$3\r\nset\r\n$3\r\nvar\r\n$3\r\nval\r\n$2\r\nex\r\n$2\r\n10\r\n"))
	f.Add([]byte("*5\r\n$7\r\nflushdb\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n")) // five parts, no TTL
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

		// Only NUL bytes may follow a fully loaded input.
		if err == nil && len(bytes.Trim(in[n:], "\x00")) != 0 {
			t.Fatalf("readLoad stopped at %d, but in[%d:]=%q is not all NUL", n, n, in[n:])
		}
		// Every command ends in '\n', so an offset after a NUL counted trailing
		// NULs. Replay below cannot see that: it repeats the same wrong offset.
		if n > 0 && in[n-1] == 0 {
			t.Fatalf("readLoad offset %d counts trailing NULs: in[:%d] ends %q", n, n, in[:n])
		}
		// in[:n] holds only complete commands and NULs, so it must reload to
		// (n, nil). That fails if n includes a partial command.
		// Keys come out the same unless a TTL record reads wall time.
		db2 := newLoadTarget()
		n2, err2 := db2.readLoad(bytes.NewReader(in[:n]), now)
		if n2 != n || err2 != nil {
			t.Fatalf("readLoad(in[:%d]) = (%d, %v), want (%d, nil); first err=%v", n, n2, err2, n, err)
		}
		if !hasTTLSet(in[:n]) && !slices.Equal(loadedKeys(db), loadedKeys(db2)) {
			t.Fatalf("keys differ after reloading in[:%d]", n)
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
	var corpus []byte
	for i := range 20000 {
		corpus = appendSet(corpus,
			fmt.Sprintf("user:%012d", i),
			fmt.Sprintf("value-%012d", i))
	}
	benchmarkLoad(b, corpus)
}

func Benchmark_Load_LargeValues(b *testing.B) {
	large := strings.Repeat("x", 1<<20) // 1 MiB
	var corpus []byte
	for i := range 8 {
		corpus = appendSet(corpus, fmt.Sprintf("big:%d", i), large)
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

// setExRecord encodes a five-part SET of key "k" with a raw expiry argument
// (arg is "ae" or "ex"). It bypasses the writer so a test can feed the loader
// numbers that the writer would never produce.
func setExRecord(arg, num string) []byte {
	return []byte("*5\r\n$3\r\nset\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\n" + arg +
		"\r\n$" + strconv.Itoa(len(num)) + "\r\n" + num + "\r\n")
}

// C3: a pre-1970 absolute expiry was written as a uint64, which the reader's
// ParseInt rejects as out of range, so the database could not reopen.
func TestRegressPre1970ExpiryReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() // a second Close after the explicit one is harmless
	if err := db.Update(func(tx *Tx) error {
		if _, _, err := tx.Set("live", "1", nil); err != nil {
			return err
		}
		_, _, err := tx.Set("old", "2", &SetOptions{Expires: true, TTL: -100 * 365 * 24 * time.Hour})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		raw, _ := os.ReadFile(path)
		t.Fatalf("reopen: %v; file=%q", err, raw)
	}
	defer db.Close()
	if got, want := dbContents(t, db), map[string]string{"live": "1"}; !maps.Equal(got, want) {
		t.Errorf("after reopen got %v, want %v", got, want)
	}
}

// The loader accepts a wrapped uint64 for "ae" only, to read files that the
// old writer produced. Every other bad number is ErrInvalid, as is an "ae"
// that time.Unix cannot represent. An accepted expiry replaces an earlier "k".
func TestRegressLoadExpiryNumbers(t *testing.T) {
	maxAe := strconv.FormatInt(maxTime.Unix(), 10)
	for _, tc := range []struct {
		name, arg, num string
		wantErr        error
		live           bool // k keeps the new value instead of being deleted
	}{
		{"signed pre-1970", "ae", "-31536000", nil, false},
		{"most negative ae", "ae", "-9223372036854775808", nil, false},
		{"old writer, wrapped uint64", "ae", "18446744073678015616", nil, false},
		{"old writer, max uint64", "ae", "18446744073709551615", nil, false},
		{"ae at maxTime", "ae", maxAe, nil, true},
		{"ae above maxTime", "ae", "9223371974719179008", ErrInvalid, false},
		{"ae max int64", "ae", "9223372036854775807", ErrInvalid, false},
		{"ae above uint64", "ae", "18446744073709551616", ErrInvalid, false},
		{"ae below int64", "ae", "-9223372036854775809", ErrInvalid, false},
		{"ae not a number", "ae", "abc", ErrInvalid, false},
		{"ae empty", "ae", "", ErrInvalid, false},
		{"ex wrapped uint64", "ex", "18446744073678015616", ErrInvalid, false},
		{"ex above int64", "ex", "9223372036854775808", ErrInvalid, false},
		{"ex not a number", "ex", "1e3", ErrInvalid, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := appendSet(nil, "k", "previous")
			rec = appendSet(rec, "other", "o")
			rec = append(rec, setExRecord(tc.arg, tc.num)...)
			db := newLoadTarget()
			_, err := db.readLoad(bytes.NewReader(rec), time.Now())
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("readLoad err = %v, want %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			want := []string{"other"}
			if tc.live {
				want = []string{"k", "other"}
			}
			if got := loadedKeys(db); !slices.Equal(got, want) {
				t.Fatalf("loaded keys %v, want %v", got, want)
			}
			if k := db.get("k"); tc.live && (k == nil || k.val != "v") {
				t.Errorf("k = %+v, want the new value %q", k, "v")
			}
		})
	}
}

// The post-1970 bytes must never change: files written by older versions and
// by this one have to stay readable by each other. A pre-1970 expiry is the
// one deliberate difference, written as a signed number.
func TestControlWriterGoldenBytes(t *testing.T) {
	const set = "*5\r\n$3\r\nset\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nae\r\n"
	withEx := func(exat time.Time) *dbItem {
		return &dbItem{key: "k", val: "v", opts: &dbItemOpts{ex: true, exat: exat}}
	}
	for _, tc := range []struct {
		name string
		item *dbItem
		want string
	}{
		{"plain set", &dbItem{key: "k", val: "v"}, "*3\r\n$3\r\nset\r\n$1\r\nk\r\n$1\r\nv\r\n"},
		{"post-1970 expiry", withEx(time.Unix(1700000000, 123)), set + "$10\r\n1700000000\r\n"},
		{"epoch expiry", withEx(time.Unix(0, 0)), set + "$1\r\n0\r\n"},
		{"pre-1970 expiry", withEx(time.Unix(-31536000, 0)), set + "$9\r\n-31536000\r\n"},
		{"year 1 expiry", withEx(time.Time{}), set + "$12\r\n-62135596800\r\n"},
		{"most negative expiry", withEx(time.Unix(math.MinInt64, 0)), set + "$20\r\n-9223372036854775808\r\n"},
		{"maxTime expiry", withEx(maxTime), set + "$19\r\n9223371974719179007\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := string(tc.item.writeSetTo(nil)); got != tc.want {
				t.Errorf("writeSetTo = %q, want %q", got, tc.want)
			}
			// The writer must not allocate once the buffer has room.
			buf := make([]byte, 0, 256)
			if n := testing.AllocsPerRun(100, func() { buf = tc.item.writeSetTo(buf[:0]) }); n != 0 {
				t.Errorf("writeSetTo allocates %v times per call, want 0", n)
			}
		})
	}
	const del = "*2\r\n$3\r\ndel\r\n$1\r\nk\r\n"
	if got := string((&dbItem{key: "k"}).writeDeleteTo(nil)); got != del {
		t.Errorf("writeDeleteTo = %q, want %q", got, del)
	}
}

// C12: a legacy relative "ex" TTL overflowed time.Duration arithmetic, so a
// hugely expired record loaded as live and a long TTL on a future-dated file
// loaded as expired. The mtime is set by Chtimes, so it needs no sleeping.
func TestRegressLegacyExFile(t *testing.T) {
	const maxEx = math.MaxInt64 / int64(time.Second) // 9223372036: the largest ex that fits
	for _, tc := range []struct {
		name string
		ex   int64
		age  time.Duration // negative: the file is dated in the future
		live bool
	}{
		{"fresh file", 3600, 0, true},
		{"one second old", 3600, time.Second, true},
		{"future-dated file", 3600, -2 * time.Second, true},
		{"future-dated, far-future ex", maxEx, -2 * time.Second, true},
		{"one second old, far-future ex", maxEx, time.Second, true},
		{"fresh, negative ex at limit", -maxEx, 0, false},
		{"one second old, negative ex at limit", -maxEx, time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "x.db")
			if err := os.WriteFile(path, setExRecord("ex", strconv.FormatInt(tc.ex, 10)), 0666); err != nil {
				t.Fatal(err)
			}
			mtime := time.Now().Add(-tc.age)
			if err := os.Chtimes(path, mtime, mtime); err != nil {
				t.Fatal(err)
			}
			fi, err := os.Stat(path) // the file system may round the time
			if err != nil {
				t.Fatal(err)
			}
			m := fi.ModTime()
			t0 := time.Now()
			db, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.View(func(tx *Tx) error {
				ttl, err := tx.TTL("k")
				t1 := time.Now()
				if !tc.live {
					if err != ErrNotFound {
						t.Errorf("TTL = (%v, %v), want ErrNotFound", ttl, err)
					}
					return nil
				}
				if err != nil {
					t.Errorf("TTL err = %v, want a live item", err)
					return nil
				}
				// The deadline is m+ex for an old file and Open's time plus ex for
				// a future-dated one, which counts as age 0. Time passes between
				// t0, Open, and t1, so the observed times bound the result. This
				// holds even if a pause makes a future-dated file old by Open.
				d := time.Duration(tc.ex) * time.Second
				hi := d - max(0, t0.Sub(m))
				lo := hi - t1.Sub(t0)
				const skew = time.Millisecond // wall clock against monotonic clock
				if ttl > hi+skew || ttl < lo-skew {
					t.Errorf("TTL = %v, want in [%v, %v]", ttl, lo-skew, hi+skew)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The boundaries of the legacy "ex" arithmetic, tested without file timing.
func TestRegressLegacyExpiryBoundaries(t *testing.T) {
	const maxEx = math.MaxInt64 / int64(time.Second) // 9223372036
	now := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	for _, tc := range []struct {
		name    string
		ex      int64
		modTime time.Time
		want    time.Time // the zero value means "expired", maxTime means "never"
	}{
		{"fresh", 10, now, now.Add(10 * time.Second)},
		{"keeps nanoseconds", 10, now.Add(-1500 * time.Millisecond), now.Add(8500 * time.Millisecond)},
		{"zero ex, fresh", 0, now, time.Time{}},
		{"zero ex, old", 0, now.Add(-time.Second), time.Time{}},
		{"older than ex", 10, now.Add(-11 * time.Second), time.Time{}},
		{"future file counts as fresh", 10, now.Add(2 * time.Second), now.Add(10 * time.Second)},
		{"far-future file counts as fresh", 10, now.AddDate(300, 0, 0), now.Add(10 * time.Second)},
		{"max ex, fresh", maxEx, now, now.Add(time.Duration(maxEx) * time.Second)},
		{"max ex, 1 s old", maxEx, now.Add(-time.Second), now.Add(time.Duration(maxEx)*time.Second - time.Second)},
		{"max ex, future file", maxEx, now.Add(2 * time.Second), now.Add(time.Duration(maxEx) * time.Second)},
		{"max ex, zero-time file", maxEx, time.Time{}, time.Time{}},
		{"max ex + 1", maxEx + 1, now, maxTime},
		{"max ex + 1, old file", maxEx + 1, now.Add(-time.Hour), maxTime},
		{"MaxInt64", math.MaxInt64, now, maxTime},
		{"min ex, fresh", -maxEx, now, time.Time{}},
		{"min ex, 1 s old", -maxEx, now.Add(-time.Second), time.Time{}},
		{"min ex, future file", -maxEx, now.Add(2 * time.Second), time.Time{}},
		{"min ex - 1", -maxEx - 1, now, time.Time{}},
		{"MinInt64", math.MinInt64, now, time.Time{}},
		{"zero-time file, ex 1", 1, time.Time{}, time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := legacyExpiry(tc.ex, tc.modTime, now)
			switch {
			case tc.want.IsZero():
				if got.After(now) {
					t.Errorf("legacyExpiry = %v, want expired (not after %v)", got, now)
				}
			case !got.Equal(tc.want):
				t.Errorf("legacyExpiry = %v, want %v", got, tc.want)
			}
		})
	}
}

// Save and Load dereferenced the nil trees of a closed database, and Load
// reported ErrPersistenceActive before it looked at the closed state.
func TestRegressSaveLoadAfterClose(t *testing.T) {
	for _, mode := range []string{"memory", "persistent"} {
		t.Run(mode, func(t *testing.T) {
			path := ":memory:"
			if mode == "persistent" {
				path = filepath.Join(t.TempDir(), "closed.db")
			}
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
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			for _, op := range []struct {
				name string
				run  func() error
			}{
				{"Save", func() error { return db.Save(io.Discard) }},
				{"Load", func() error { return db.Load(bytes.NewReader(appendSet(nil, "k2", "v2"))) }},
			} {
				var err error
				var panicked any
				func() {
					defer func() { panicked = recover() }()
					err = op.run()
				}()
				if panicked != nil {
					t.Errorf("%s after Close panicked: %v", op.name, panicked)
				} else if err != ErrDatabaseClosed {
					t.Errorf("%s after Close = %v, want ErrDatabaseClosed", op.name, err)
				}
			}
		})
	}
}

// spatialAPI runs one spatial search with a key-only callback, so a test can
// treat Intersects and Nearby alike. The query covers every fixture point.
type spatialAPI struct {
	name string
	run  func(tx *Tx, fn func(key string) bool) error
}

var spatialAPIs = []spatialAPI{
	{"Intersects", func(tx *Tx, fn func(string) bool) error {
		return tx.Intersects("pts", "[0 0],[100 100]", func(key, _ string) bool { return fn(key) })
	}},
	{"Nearby", func(tx *Tx, fn func(string) bool) error {
		return tx.Nearby("pts", "[0 0]", func(key, _ string, _ float64) bool { return fn(key) })
	}},
}

// setupExpiredSpatial creates the index "pts" and three 2-D points: p1 is
// already expired, and is both first in insertion order and nearest to [0 0];
// p2 has no TTL; p3 has a TTL that has not passed. It runs in tx, so the
// background manager cannot sweep p1 before the test searches.
func setupExpiredSpatial(tx *Tx) error {
	if err := tx.CreateSpatialIndex("pts", "*", IndexRect); err != nil {
		return err
	}
	for _, p := range []struct {
		key  string
		xy   float64
		opts *SetOptions
	}{
		{"p1", 1, &SetOptions{Expires: true, TTL: -time.Hour}},
		{"p2", 2, nil},
		{"p3", 3, &SetOptions{Expires: true, TTL: time.Hour}},
	} {
		if _, _, err := tx.Set(p.key, Point(p.xy, p.xy), p.opts); err != nil {
			return err
		}
	}
	if _, err := tx.Get("p1"); err != ErrNotFound {
		return fmt.Errorf("Get of the expired point = %v, want ErrNotFound", err)
	}
	return nil
}

// Intersects and Nearby returned expired items that Get hides. The fixture
// never waits on the clock, and it keeps p1 in the tree until the search runs.
func TestRegressSpatialSkipsExpired(t *testing.T) {
	// check runs every search in tx. Nearby order is exact. Intersects order is
	// not specified, so it compares sorted keys.
	check := func(t *testing.T, tx *Tx) error {
		for _, api := range spatialAPIs {
			var got []string
			if err := api.run(tx, func(key string) bool {
				got = append(got, key)
				return true
			}); err != nil {
				return fmt.Errorf("%s: %w", api.name, err)
			}
			if api.name == "Intersects" {
				slices.Sort(got)
			}
			if want := []string{"p2", "p3"}; !slices.Equal(got, want) {
				t.Errorf("%s returned %v, want %v", api.name, got, want)
			}

			// The callback's false must stop the search after the first live
			// item. A wrapper that drops it makes more than one call.
			var calls int
			var first string
			if err := api.run(tx, func(key string) bool {
				calls++
				first = key
				return false
			}); err != nil {
				return fmt.Errorf("%s: %w", api.name, err)
			}
			if calls != 1 || (first != "p2" && first != "p3") {
				t.Errorf("%s with early stop: %d calls, first key %q; want 1 call, a live key",
					api.name, calls, first)
			}
		}
		return nil
	}

	t.Run("writable", func(t *testing.T) {
		db, err := Open(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if err := db.Update(func(tx *Tx) error {
			if err := setupExpiredSpatial(tx); err != nil {
				return err
			}
			return check(t, tx)
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("read-only", func(t *testing.T) {
		// A DB without the background manager keeps the expired item.
		db := newLoadTarget()
		if err := db.Update(setupExpiredSpatial); err != nil {
			t.Fatal(err)
		}
		if err := db.View(func(tx *Tx) error { return check(t, tx) }); err != nil {
			t.Fatal(err)
		}
	})
}

// Set and Delete were allowed inside Intersects and Nearby callbacks, which
// changed the tree under the traversal. Each callback below runs once and
// returns false, so unfixed code cannot traverse a tree it has changed.
func TestRegressSpatialWriteDuringIteration(t *testing.T) {
	mutations := []struct {
		name string
		run  func(tx *Tx) error
	}{
		{"Set", func(tx *Tx) error { _, _, err := tx.Set("p2", Point(2, 2), nil); return err }},
		{"Delete", func(tx *Tx) error { _, err := tx.Delete("p1"); return err }},
	}
	newDB := func(t *testing.T) *DB {
		t.Helper()
		db, err := Open(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		})
		if err := db.Update(func(tx *Tx) error {
			if err := tx.CreateSpatialIndex("pts", "*", IndexRect); err != nil {
				return err
			}
			_, _, err := tx.Set("p1", Point(1, 1), nil)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return db
	}

	for _, api := range spatialAPIs {
		for _, mut := range mutations {
			t.Run(api.name+"/"+mut.name+"/writable", func(t *testing.T) {
				db := newDB(t)
				if err := db.Update(func(tx *Tx) error {
					var calls int
					var inErr error
					if err := api.run(tx, func(string) bool {
						calls++
						inErr = mut.run(tx)
						return false
					}); err != nil {
						return err
					}
					if calls != 1 || inErr != ErrTxIterating {
						t.Errorf("%d callbacks, %s inside = %v; want 1 callback, ErrTxIterating",
							calls, mut.name, inErr)
					}
					// The traversal has ended, so the same mutation must work now.
					if err := mut.run(tx); err != nil {
						t.Errorf("%s after the search = %v, want nil", mut.name, err)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			})
			t.Run(api.name+"/"+mut.name+"/read-only", func(t *testing.T) {
				db := newDB(t)
				if err := db.View(func(tx *Tx) error {
					var calls int
					var inErr error
					if err := api.run(tx, func(string) bool {
						calls++
						inErr = mut.run(tx)
						return false
					}); err != nil {
						return err
					}
					if calls != 1 || inErr != ErrTxNotWritable {
						t.Errorf("%d callbacks, %s inside = %v; want 1 callback, ErrTxNotWritable",
							calls, mut.name, inErr)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}

	// A write is still rejected after an inner traversal returns, while the
	// outer callback is active. A counter reset by the inner exit would allow it.
	for _, outer := range spatialAPIs {
		for _, inner := range spatialAPIs {
			t.Run("nested/"+outer.name+"/"+inner.name, func(t *testing.T) {
				db := newDB(t)
				if err := db.Update(func(tx *Tx) error {
					var calls, innerCalls int
					var afterInner error
					if err := outer.run(tx, func(string) bool {
						calls++
						if err := inner.run(tx, func(string) bool {
							innerCalls++
							return false
						}); err != nil {
							t.Errorf("inner %s = %v", inner.name, err)
						}
						_, _, afterInner = tx.Set("p2", Point(2, 2), nil)
						return false
					}); err != nil {
						return err
					}
					if calls != 1 || innerCalls != 1 || afterInner != ErrTxIterating {
						t.Errorf("%d outer, %d inner callbacks, Set after inner = %v; want 1, 1, ErrTxIterating",
							calls, innerCalls, afterInner)
					}
					if _, _, err := tx.Set("p2", Point(2, 2), nil); err != nil {
						t.Errorf("Set after both searches = %v, want nil", err)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}

	// A panic in the callback must release the counter. The recover is in the
	// same Update callback, so a stuck counter would reject the final Set. A
	// new transaction would hide that.
	for _, api := range spatialAPIs {
		t.Run("panic/"+api.name, func(t *testing.T) {
			db := newDB(t)
			const sentinel = "spatial callback panic"
			if err := db.Update(func(tx *Tx) error {
				var recovered any
				var inErr error
				var calls int
				func() {
					defer func() { recovered = recover() }()
					_ = api.run(tx, func(string) bool {
						calls++
						_, _, inErr = tx.Set("p2", Point(2, 2), nil)
						panic(sentinel)
					})
				}()
				if recovered != sentinel || calls != 1 {
					t.Errorf("recovered %v after %d callbacks, want %q after 1", recovered, calls, sentinel)
				}
				if inErr != ErrTxIterating {
					t.Errorf("Set inside the callback = %v, want ErrTxIterating", inErr)
				}
				if _, _, err := tx.Set("p2", Point(2, 2), nil); err != nil {
					t.Errorf("Set after the panic = %v, want nil (iteration counter stuck)", err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// lazyClock must read the clock at most once per traversal, and only for an
// item that can expire.
func TestLazyClockReadsOnce(t *testing.T) {
	var c lazyClock
	if c.expired(&dbItem{key: "a"}) || c.expired(&dbItem{key: "b", opts: &dbItemOpts{}}) {
		t.Error("an item without a TTL reported expired")
	}
	if !c.now.IsZero() {
		t.Error("the clock was read for items without a TTL")
	}

	live := &dbItem{key: "c", opts: &dbItemOpts{ex: true, exat: time.Now().Add(time.Hour)}}
	if c.expired(live) {
		t.Error("an item with an hour left reported expired")
	}
	if c.now.IsZero() {
		t.Fatal("the clock was not read for an item with a TTL")
	}

	// A later reading would say live, so only the stored reading can say expired.
	c.now = time.Now().Add(2 * time.Hour)
	stored := c.now
	if !c.expired(live) {
		t.Error("the stored reading was not used")
	}
	if !c.now.Equal(stored) {
		t.Error("the clock was read again")
	}
}
