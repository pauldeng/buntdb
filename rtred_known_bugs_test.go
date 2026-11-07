package buntdb

// Opt-in reproducers for two defects in github.com/tidwall/rtred v0.1.2.
// BuntDB documents both and does not work around them. They run only when
// BUNTDB_RTRED_KNOWN_BUGS=1, and they fail while the defect exists.
//
// A passing run means "not observed". It does not refute a defect: both are
// visible in the rtred source, and the first is also modelled exhaustively in
// bench/round12/d3_merge_model.py.
//
//   - D3: KNN merges the per-dimension queues without advancing its loop index
//     (rtree.go:235-245), so an index with rects of different dimensions can
//     return Nearby results out of order.
//   - D4: with more than one dimensionality, KNN locks a mutex (rtree.go:219),
//     calls the user callback (:247), and unlocks only on the normal path
//     (:255). A panic or runtime.Goexit leaves the mutex locked and the
//     producer goroutines blocked. After an early stop, nothing joins them.
//
// When a defect is fixed, remove its opt-in and make the test a normal test.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	knownBugsEnv = "BUNTDB_RTRED_KNOWN_BUGS"
	probeModeEnv = "BUNTDB_RTRED_PROBE_MODE" // set only for the D4 child process
	rtredKNNFunc = "github.com/tidwall/rtred.(*RTree).KNN"
)

func requireKnownBugs(t *testing.T) {
	t.Helper()
	if os.Getenv(knownBugsEnv) != "1" {
		t.Skipf("opt-in reproducer for a known rtred defect; set %s=1 to run it", knownBugsEnv)
	}
}

// TestKnownBugRtredMixedDimensionOrder reproduces D3. The two distance orders
// are 1-D: 1, 10000 and 2-D: 4, 2500, so the only correct order interleaves
// them. The merge returns the two 2-D items before the 1-D ones.
func TestKnownBugRtredMixedDimensionOrder(t *testing.T) {
	requireKnownBugs(t)
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := db.Update(func(tx *Tx) error {
		if err := tx.CreateSpatialIndex("pts", "p:*", IndexRect); err != nil {
			return err
		}
		for _, p := range []struct{ key, rect string }{
			{"p:near1d", "[1]"}, {"p:far1d", "[100]"},
			{"p:near2d", "[2 0]"}, {"p:far2d", "[50 0]"},
		} {
			if _, _, err := tx.Set(p.key, p.rect, nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	wantOrder := []string{"p:near1d:1", "p:near2d:4", "p:far2d:2500", "p:far1d:10000"}
	const rounds = 500
	var badOrder, badTop2 int
	var firstOrder, firstTop2 string
	for range rounds {
		var got []string
		if err := db.View(func(tx *Tx) error {
			return tx.Nearby("pts", "[0 0]", func(key, _ string, dist float64) bool {
				got = append(got, fmt.Sprintf("%s:%g", key, dist))
				return true
			})
		}); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, wantOrder) {
			if badOrder++; badOrder == 1 {
				firstOrder = fmt.Sprint(got)
			}
		}

		// Early termination at k=2 must return the two nearest items.
		var top2 []string
		if err := db.View(func(tx *Tx) error {
			return tx.Nearby("pts", "[0 0]", func(key, _ string, dist float64) bool {
				top2 = append(top2, fmt.Sprintf("%s:%g", key, dist))
				return len(top2) < 2
			})
		}); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(top2, wantOrder[:2]) {
			if badTop2++; badTop2 == 1 {
				firstTop2 = fmt.Sprint(top2)
			}
		}
	}
	if badOrder > 0 {
		t.Errorf("D3: Nearby order wrong in %d of %d queries; want %v; first: %s",
			badOrder, rounds, wantOrder, firstOrder)
	}
	if badTop2 > 0 {
		t.Errorf("D3: top-2 wrong in %d of %d queries; want %v; first: %s",
			badTop2, rounds, wantOrder[:2], firstTop2)
	}
	if badOrder == 0 && badTop2 == 0 {
		t.Logf("D3 not observed in %d rounds; the defect is in rtred@v0.1.2/rtree.go:235-245", rounds)
	}
}

// TestKnownBugRtredNearbyPanicLeak reproduces D4 for callback panics.
func TestKnownBugRtredNearbyPanicLeak(t *testing.T) { runD4Probe(t, "panic") }

// TestKnownBugRtredNearbyEarlyStopLeak reproduces D4 for an early stop.
func TestKnownBugRtredNearbyEarlyStopLeak(t *testing.T) { runD4Probe(t, "earlystop") }

// runD4Probe runs the probe in a child process, because a leak is destructive
// to the test process: the leaked goroutines never exit.
func runD4Probe(t *testing.T, mode string) {
	requireKnownBugs(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.v", "-test.run=^TestKnownBugRtredProbeChild$")
	cmd.Env = append(os.Environ(), probeModeEnv+"="+mode)
	out, err := cmd.CombinedOutput()
	if !strings.Contains(string(out), "--- PASS: TestKnownBugRtredProbeChild") &&
		!strings.Contains(string(out), "--- FAIL: TestKnownBugRtredProbeChild") {
		t.Fatalf("probe child did not run (err=%v):\n%s", err, out)
	}
	if err != nil {
		t.Errorf("D4 %s probe failed (err=%v):\n%s", mode, err, out)
		return
	}
	t.Logf("D4 %s: not observed:\n%s", mode, out)
}

// TestKnownBugRtredProbeChild is the child process for runD4Probe. It does
// nothing in a normal run.
func TestKnownBugRtredProbeChild(t *testing.T) {
	mode := os.Getenv(probeModeEnv)
	if mode == "" {
		t.Skip("child process of the D4 reproducers")
	}
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	// A leaked producer holds no database lock, so Close still returns.
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	// Big trees keep the producers busy when the callback panics or stops.
	const n = 50000
	if err := db.Update(func(tx *Tx) error {
		if err := tx.CreateSpatialIndex("pts", "p:*", IndexRect); err != nil {
			return err
		}
		for i := range n {
			if _, _, err := tx.Set(fmt.Sprintf("p:2d:%d", i), Point(float64(i%1000), float64(i/1000)), nil); err != nil {
				return err
			}
			if _, _, err := tx.Set(fmt.Sprintf("p:1d:%d", i), Point(float64(i)), nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	const attempts = 20
	type sentinel struct{}
	switch mode {
	case "panic":
		for i := range attempts {
			var calls int
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				// View releases its lock when the callback panics.
				_ = db.View(func(tx *Tx) error {
					return tx.Nearby("pts", "[0 0]", func(string, string, float64) bool {
						calls++
						panic(sentinel{})
					})
				})
			}()
			if calls != 1 || recovered != (sentinel{}) {
				t.Fatalf("attempt %d: %d callbacks, recovered %v; want 1 callback and the sentinel", i, calls, recovered)
			}
		}
		time.Sleep(500 * time.Millisecond) // let any producer that can finish do so
		if stacks := knnStacks(); len(stacks) > 0 {
			t.Errorf("D4: %d goroutines still in %s after %d panics; first:\n%s",
				len(stacks), rtredKNNFunc, attempts, stacks[0])
		}
	case "earlystop":
		var leaked int
		for i := range attempts {
			var calls int
			if err := db.View(func(tx *Tx) error {
				return tx.Nearby("pts", "[0 0]", func(string, string, float64) bool {
					calls++
					return false
				})
			}); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("attempt %d: %d callbacks, want 1", i, calls)
			}
			// Check right after Nearby returns, without waiting.
			if len(knnStacks()) > 0 {
				leaked++
				// Let this attempt's producers finish, so the next attempt
				// does not count them again.
				for range 50 {
					if len(knnStacks()) == 0 {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
		if leaked > 0 {
			t.Errorf("D4: producer goroutines were still alive right after Nearby returned in %d of %d early stops",
				leaked, attempts)
		}
	default:
		t.Fatalf("unknown probe mode %q", mode)
	}
}

// knnStacks returns the stacks of the goroutines that are inside rtred's KNN.
func knnStacks() []string {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf)) // the stacks were truncated
	}
	var out []string
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, rtredKNNFunc) {
			out = append(out, g)
		}
	}
	return out
}
