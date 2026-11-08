// Package buntdb implements a low-level in-memory key/value store in pure Go.
//
// It persists to disk and is ACID compliant. It uses locking for multiple
// readers and one writer.
//
// BuntDB is ideal for projects that need a dependable database and favor speed
// over data size.
package buntdb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"iter"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/btree/v2"
	"github.com/tidwall/gjson"
	"github.com/tidwall/grect"
	"github.com/tidwall/match"
	"github.com/tidwall/rtred"
)

var (
	// ErrTxNotWritable is the error that a write operation returns in a read-only
	// transaction. Commit returns it for a read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is the error that an operation returns when its transaction
	// already ended with Commit or Rollback.
	ErrTxClosed = errors.New("tx closed")

	// ErrNotFound is the error that an operation returns when an index or item is
	// not in the database, or when an item has expired. An operation also returns
	// it when the index has no less function or rect function that the operation
	// needs.
	ErrNotFound = errors.New("not found")

	// ErrInvalid is the error that Open and Load return when the data has an
	// invalid format.
	ErrInvalid = errors.New("invalid database")

	// ErrDatabaseClosed is the error that an operation returns when the database
	// is closed.
	ErrDatabaseClosed = errors.New("database closed")

	// ErrIndexExists is the error that the index-creating functions return when
	// an index with the same name exists, or when the name is empty.
	ErrIndexExists = errors.New("index exists")

	// ErrInvalidOperation is the error that DropIndex, ReplaceIndex and
	// ReplaceSpatialIndex return when the index name is empty.
	ErrInvalidOperation = errors.New("invalid operation")

	// ErrInvalidSyncPolicy is the error that SetConfig returns when SyncPolicy is
	// not Never, EverySecond or Always.
	ErrInvalidSyncPolicy = errors.New("invalid sync policy")

	// ErrShrinkInProcess is the error that Shrink returns when another Shrink is
	// running.
	ErrShrinkInProcess = errors.New("shrink is in-process")

	// ErrPersistenceActive is the error that Load returns when the database
	// persists to disk. Only an in-memory database, from Open(":memory:"),
	// accepts Load.
	ErrPersistenceActive = errors.New("persistence active")

	// ErrTxIterating is the error that a write operation returns inside an
	// iteration callback. Set, Delete, DeleteAll, DropIndex and the
	// index-creating functions return it while an Ascend, Descend, Intersects or
	// Nearby callback runs. In a read-only transaction, these operations return
	// ErrTxNotWritable instead.
	ErrTxIterating = errors.New("tx is iterating")
)

// DB is a collection of items. An item is a key and its value. A transaction
// gives all access to the items of a DB. A DB persists to a database file,
// unless Open receives the path ":memory:".
type DB struct {
	mu        sync.RWMutex      // the gatekeeper for all fields, except synced
	file      *os.File          // the underlying file
	buf       []byte            // a buffer to write to
	keys      *dbTree           // a tree of all item ordered by key
	exps      *dbTree           // a tree of items ordered by expiration
	idxs      map[string]*index // the index trees.
	insIdxs   []*index          // a reuse buffer for gathering indexes
	flushes   int64             // a count of the number of disk flushes
	synced    atomic.Int64      // atomic, not under mu: the largest flushes value acknowledged by a successful file fsync
	path      string            // the absolute path of the file; its parent is resolved at Open
	dir       string            // the real directory that holds the file; Sync syncs it. Set at Open, updated when Shrink replaces a symlink
	closed    bool              // set when the database has been closed
	config    Config            // the database configuration
	persist   bool              // do we write to disk
	shrinking bool              // when an aof shrink is in-process.
	lastaofsz int64             // the size of the last shrink aof size
}

// SyncPolicy sets how often BuntDB syncs the database file to disk. A sync
// asks the operating system to write the file data to the storage device. On
// Unix, it is an fsync call.
type SyncPolicy int

const (
	// Never does not sync the database file after a commit or in the background.
	// The operating system decides when it writes the data to disk. Close and
	// DB.Sync still sync the file. This policy is the fastest and the least safe.
	Never SyncPolicy = 0
	// EverySecond syncs the database file in the background about once each
	// second, when a commit still needs a sync. The background task can run late,
	// so the policy guarantees no maximum loss window. A crash can lose the
	// commits since the last sync. This policy is the default.
	EverySecond = 1
	// Always syncs the database file after every commit that writes to the file.
	// This policy is the slowest and the safest. Commit returns a failed sync.
	// See Commit for what that error means.
	Always = 2
)

// Config holds the configuration options of a database. The options change how
// the database behaves.
type Config struct {
	// SyncPolicy sets how often BuntDB syncs the database file to disk. The value
	// is Never, EverySecond or Always. Open sets EverySecond.
	SyncPolicy SyncPolicy

	// AutoShrinkPercentage sets when the background task shrinks the database
	// file. The task shrinks the file when it grows by more than this percentage
	// of its size after the previous shrink.
	//
	// For example, assume the value is 100 and the previous shrink made a 100 MiB
	// file. The file must then be larger than 200 MiB before a shrink starts.
	//
	// A value of zero or less means that any growth qualifies. The file must also
	// be larger than AutoShrinkMinSize. Open sets 100.
	AutoShrinkPercentage int

	// AutoShrinkMinSize sets the size, in bytes, that the database file must
	// exceed before the background task can shrink it. Open sets 32 MiB.
	AutoShrinkMinSize int

	// AutoShrinkDisabled turns off automatic background shrinking. Open sets
	// false.
	AutoShrinkDisabled bool

	// OnExpired is an optional callback that receives the keys of expired items.
	// The background task calls it outside any transaction.
	//
	// BuntDB does not delete the expired items when OnExpired is set. The
	// callback controls them. It can delete an item, set it again with a new TTL,
	// or set it again without a TTL. At each run, the task reports the items that
	// are still expired.
	OnExpired func(keys []string)

	// OnExpiredSync is an optional callback that the background task calls for
	// each expired item. It runs inside the transaction that handles the expired
	// items. BuntDB does not call it when OnExpired is set.
	//
	// BuntDB does not delete the item. The callback controls it through tx. It
	// can delete the item, set it again with a new TTL, or set it again without a
	// TTL. If the callback returns an error, BuntDB rolls back the transaction.
	OnExpiredSync func(key, value string, tx *Tx) error
}

// initTrees gives the database empty keys and expiration trees and an empty
// index map. Used by Open and by a FLUSHDB record during load.
func (db *DB) initTrees() {
	db.keys = btreeNew(lessByKey)
	db.exps = btreeNew(lessExps)
	db.idxs = make(map[string]*index)
}

// Open opens the database at path. It creates the database file, with mode
// 0666 before the umask, if the file does not exist. The path ":memory:" opens
// a database that does not persist to disk.
//
// Open replays the file into the item trees. If the file ends in the middle of
// a command, Open truncates the file after the last complete command. If the
// file has an invalid format, Open returns ErrInvalid.
//
// Open starts a background task. The task runs about once each second.
func Open(path string) (*DB, error) {
	db := &DB{}
	// initialize trees and indexes
	db.initTrees()
	// initialize default configuration
	db.config = Config{
		SyncPolicy:           EverySecond,
		AutoShrinkPercentage: 100,
		AutoShrinkMinSize:    32 * 1024 * 1024,
	}
	// turn off persistence for pure in-memory
	db.persist = path != ":memory:"
	if db.persist {
		// Fix the path now, so that a later chdir or symlink change cannot
		// move it. Resolve the parent first, then open through the result.
		dir, base, err := absParent(path)
		if err != nil {
			return nil, err
		}
		dir, err = filepath.EvalSymlinks(dir)
		if err != nil {
			return nil, err
		}
		db.path = filepath.Join(dir, base)
		// hardcoding 0666 as the default mode.
		db.file, err = os.OpenFile(db.path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		// A final symlink may lead to another directory, where the file lives.
		real, err := filepath.EvalSymlinks(db.path)
		if err != nil {
			_ = db.file.Close()
			return nil, err
		}
		db.dir = filepath.Dir(real)
		// load the database from disk
		if err := db.load(); err != nil {
			// close on error, ignore close error
			_ = db.file.Close()
			return nil, err
		}
	}
	// start the background manager.
	if tickInterval > 0 {
		go db.backgroundManager(tickInterval)
	}
	return db, nil
}

// Close shuts down the database. For a database that persists to disk, it
// syncs the database file and then closes the file. It also releases the item
// and index trees.
//
// Close waits for open transactions to end. Do not call Close inside a
// transaction.
//
// If the sync or the close fails, Close returns the error, or both errors
// joined. In all cases, the database is closed afterward. Close returns
// ErrDatabaseClosed if the database is already closed.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	var err error
	if db.persist {
		err = db.file.Sync()
		err = errors.Join(err, db.file.Close())
	}
	db.abandon()
	return err
}

// Save writes a snapshot of the database to wr. Save blocks writes, but not
// reads, while it runs. It returns the first error from wr.
//
// Use Save to back up a database. It works for in-memory databases and for
// databases that persist to disk. A copy of the database file of a live
// database can hold a partial transaction. Copy the file only when the
// database is closed.
//
// Save returns ErrDatabaseClosed if the database is closed.
func (db *DB) Save(wr io.Writer) error {
	var err error
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	// use a buffered writer and flush every 4MB
	var buf []byte
	// iterated through every item in the database and write to the buffer
	btreeAscend(db.keys, func(dbi *dbItem) bool {
		buf = dbi.writeSetTo(buf)
		if len(buf) > 1024*1024*4 {
			// flush when buffer is over 4MB
			_, err = wr.Write(buf)
			if err != nil {
				return false
			}
			buf = buf[:0]
		}
		return true
	})
	if err != nil {
		return err
	}
	// one final flush
	if len(buf) > 0 {
		_, err = wr.Write(buf)
		if err != nil {
			return err
		}
	}
	return nil
}

// Load reads commands from rd into the database. Load blocks all reads and
// writes while it runs.
//
// Load works only for a database that Open(":memory:") opened. If an error
// stops Load, the database keeps the items that Load read before the error. If
// rd ends in the middle of a command, Load returns io.ErrUnexpectedEOF.
//
// Load returns ErrDatabaseClosed if the database is closed. It checks this
// before it returns ErrPersistenceActive.
func (db *DB) Load(rd io.Reader) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	if db.persist {
		// cannot load into databases that persist to disk
		return ErrPersistenceActive
	}
	_, err := db.readLoad(rd, time.Now())
	return err
}

// index represents a b-tree or r-tree index and also acts as the
// b-tree/r-tree context for itself.
type index struct {
	btr     *dbTree                                // contains the items
	rtr     *rtred.RTree                           // contains the items
	name    string                                 // name of the index
	pattern string                                 // a required key pattern
	less    func(a, b string) bool                 // less comparison function
	rect    func(item string) (min, max []float64) // rect from string function
	db      *DB                                    // the origin database
	opts    IndexOptions                           // index options
}

// match matches the pattern to the key
func (idx *index) match(key string) bool {
	if idx.pattern == "*" {
		return true
	}
	if idx.opts.CaseInsensitiveKeyMatching {
		for i := 0; i < len(key); i++ {
			if key[i] >= 'A' && key[i] <= 'Z' {
				key = strings.ToLower(key)
				break
			}
		}
	}
	return match.Match(key, idx.pattern)
}

// clearCopy creates a copy of the index, but with an empty dataset.
func (idx *index) clearCopy() *index {
	// copy the index meta information
	nidx := &index{
		name:    idx.name,
		pattern: idx.pattern,
		db:      idx.db,
		less:    idx.less,
		rect:    idx.rect,
		opts:    idx.opts,
	}
	// initialize with empty trees
	if nidx.less != nil {
		nidx.btr = btreeNew(nidx.lessItems)
	}
	if nidx.rect != nil {
		nidx.rtr = rtred.New(nidx)
	}
	return nidx
}

// rebuild rebuilds the index
func (idx *index) rebuild() {
	// initialize trees
	if idx.less != nil {
		idx.btr = btreeNew(idx.lessItems)
	}
	if idx.rect != nil {
		idx.rtr = rtred.New(idx)
	}
	// iterate through all keys and fill the index
	btreeAscend(idx.db.keys, func(dbi *dbItem) bool {
		if !idx.match(dbi.key) {
			// does not match the pattern, continue
			return true
		}
		if idx.less != nil {
			idx.btr.Set(dbi)
		}
		if idx.rect != nil {
			idx.rtr.Insert(dbi)
		}
		return true
	})
}

// CreateIndex builds a new index in its own read/write transaction. See
// Tx.CreateIndex for the arguments and the errors.
func (db *DB) CreateIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return db.Update(func(tx *Tx) error {
		return tx.CreateIndex(name, pattern, less...)
	})
}

// ReplaceIndex builds a new index and fills it with items, in its own
// read/write transaction. If an index with the same name exists, ReplaceIndex
// drops that index first. It returns ErrInvalidOperation if name is empty. See
// Tx.CreateIndex for the arguments and the other errors.
func (db *DB) ReplaceIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return db.Update(func(tx *Tx) error {
		err := tx.CreateIndex(name, pattern, less...)
		if err != nil {
			if err == ErrIndexExists {
				err := tx.DropIndex(name)
				if err != nil {
					return err
				}
				return tx.CreateIndex(name, pattern, less...)
			}
			return err
		}
		return nil
	})
}

// CreateSpatialIndex builds a new spatial index in its own read/write
// transaction. See Tx.CreateSpatialIndex for the arguments and the errors.
func (db *DB) CreateSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return db.Update(func(tx *Tx) error {
		return tx.CreateSpatialIndex(name, pattern, rect)
	})
}

// ReplaceSpatialIndex builds a new spatial index and fills it with items, in
// its own read/write transaction. If an index with the same name exists,
// ReplaceSpatialIndex drops that index first. It returns ErrInvalidOperation
// if name is empty. See Tx.CreateSpatialIndex for the arguments and the other
// errors.
func (db *DB) ReplaceSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return db.Update(func(tx *Tx) error {
		err := tx.CreateSpatialIndex(name, pattern, rect)
		if err != nil {
			if err == ErrIndexExists {
				err := tx.DropIndex(name)
				if err != nil {
					return err
				}
				return tx.CreateSpatialIndex(name, pattern, rect)
			}
			return err
		}
		return nil
	})
}

// DropIndex removes an index, in its own read/write transaction. See
// Tx.DropIndex for the errors.
func (db *DB) DropIndex(name string) error {
	return db.Update(func(tx *Tx) error {
		return tx.DropIndex(name)
	})
}

// Indexes returns the names of the indexes in sorted order.
func (db *DB) Indexes() ([]string, error) {
	var names []string
	var err = db.View(func(tx *Tx) error {
		var err error
		names, err = tx.Indexes()
		return err
	})
	return names, err
}

// ReadConfig copies the database configuration to config. It returns
// ErrDatabaseClosed if the database is closed.
func (db *DB) ReadConfig(config *Config) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	*config = db.config
	return nil
}

// SetConfig replaces the database configuration. It returns
// ErrInvalidSyncPolicy if SyncPolicy is not valid. It returns
// ErrDatabaseClosed if the database is closed.
func (db *DB) SetConfig(config Config) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	switch config.SyncPolicy {
	default:
		return ErrInvalidSyncPolicy
	case Never, EverySecond, Always:
	}
	db.config = config
	return nil
}

// insertIntoDatabase performs inserts an item in to the database and updates
// all indexes. If a previous item with the same key already exists, that item
// will be replaced with the new one, and return the previous item.
func (db *DB) insertIntoDatabase(item *dbItem) *dbItem {
	var pdbi *dbItem
	// Generate a list of indexes that this item will be inserted in to.
	idxs := db.insIdxs
	for _, idx := range db.idxs {
		if idx.match(item.key) {
			idxs = append(idxs, idx)
		}
	}
	prev, _ := db.keys.Set(item)
	if prev != nil {
		// A previous item was removed from the keys tree. Let's
		// fully delete this item from all indexes.
		pdbi = prev
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the expires tree.
			db.exps.Delete(pdbi)
		}
		for _, idx := range idxs {
			if idx.btr != nil {
				// Remove it from the btree index.
				idx.btr.Delete(pdbi)
			}
			if idx.rtr != nil {
				// Remove it from the rtree index.
				idx.rtr.Remove(pdbi)
			}
		}
	}
	if item.opts != nil && item.opts.ex {
		// The new item has eviction options. Add it to the
		// expires tree
		db.exps.Set(item)
	}
	for i, idx := range idxs {
		if idx.btr != nil {
			// Add new item to btree index.
			idx.btr.Set(item)
		}
		if idx.rtr != nil {
			// Add new item to rtree index.
			idx.rtr.Insert(item)
		}
		// clear the index
		idxs[i] = nil
	}
	// reuse the index list slice
	db.insIdxs = idxs[:0]
	// we must return the previous item to the caller.
	return pdbi
}

// deleteFromDatabase removes and item from the database and indexes. The input
// item must only have the key field specified thus "&dbItem{key: key}" is all
// that is needed to fully remove the item with the matching key. If an item
// with the matching key was found in the database, it will be removed and
// returned to the caller. A nil return value means that the item was not
// found in the database
func (db *DB) deleteFromDatabase(item *dbItem) *dbItem {
	var pdbi *dbItem
	prev, _ := db.keys.Delete(item)
	if prev != nil {
		pdbi = prev
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the exipres tree.
			db.exps.Delete(pdbi)
		}
		for _, idx := range db.idxs {
			if !idx.match(pdbi.key) {
				continue
			}
			if idx.btr != nil {
				// Remove it from the btree index.
				idx.btr.Delete(pdbi)
			}
			if idx.rtr != nil {
				// Remove it from the rtree index.
				idx.rtr.Remove(pdbi)
			}
		}
	}
	return pdbi
}

// backgroundManager runs continuously in the background and performs various
// operations such as removing expired items and syncing to disk.
func (db *DB) backgroundManager(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for range t.C {
		if db.tick((*os.File).Sync) {
			break
		}
	}
}

// tickInterval is the period of the background manager, read by Open. Zero
// starts no manager: tests use it and call tick by hand.
var tickInterval = time.Second

// tick runs one background pass: the expiry sweep, the EverySecond fsync and
// the auto shrink. It returns true when the database is closed.
// syncFile is the fsync; tests pass one that holds or fails.
func (db *DB) tick(syncFile func(*os.File) error) bool {
	// Look first with the read lock, so that a tick with no expired
	// item does not block readers.
	var shrink, due bool
	db.mu.RLock()
	closed := db.closed
	if !closed {
		if first, ok := db.exps.Front(); ok {
			due = db.exps.less(first, &dbItem{
				opts: &dbItemOpts{ex: true, exat: time.Now()},
			})
		}
		if db.persist && !db.config.AutoShrinkDisabled {
			// Seek(0, SeekCurrent) only reads the offset. Writers move it
			// under the write lock.
			pos, err := db.file.Seek(0, io.SeekCurrent)
			shrink = err == nil && pos > int64(db.config.AutoShrinkMinSize) &&
				shrinkDue(pos, db.lastaofsz, db.config.AutoShrinkPercentage)
		}
	}
	db.mu.RUnlock()
	if closed {
		return true
	}
	var onExpired func([]string)
	var expired []*dbItem
	var onExpiredSync func(key, value string, tx *Tx) error
	var err error
	if due {
		err = db.Update(func(tx *Tx) error {
			onExpired = db.config.OnExpired
			if onExpired == nil {
				onExpiredSync = db.config.OnExpiredSync
			}
			// produce a list of expired items that need removing
			btreeAscendLessThan(db.exps, &dbItem{
				opts: &dbItemOpts{ex: true, exat: time.Now()},
			}, func(item *dbItem) bool {
				expired = append(expired, item)
				return true
			})
			if onExpired == nil && onExpiredSync == nil {
				for _, itm := range expired {
					if _, err := tx.Delete(itm.key); err != nil {
						// it's ok to get a "not found" because the
						// 'Delete' method reports "not found" for
						// expired items.
						if err != ErrNotFound {
							return err
						}
					}
				}
			} else if onExpiredSync != nil {
				for _, itm := range expired {
					if err := onExpiredSync(itm.key, itm.val, tx); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	if err == ErrDatabaseClosed {
		return true
	}

	// send expired event, if needed
	if onExpired != nil && len(expired) > 0 {
		keys := make([]string, 0, 32)
		for _, itm := range expired {
			keys = append(keys, itm.key)
		}
		onExpired(keys)
	}

	// Execute a disk sync, if needed. The fsync runs without a lock, so
	// readers and writers do not wait for it. A concurrent Shrink or Close
	// may close f, and the sync may then fail. A failed sync does not
	// advance db.synced, so the next tick retries while the database is open.
	db.mu.RLock()
	f, n := db.file, db.flushes
	syncDue := !db.closed && db.persist &&
		db.config.SyncPolicy == EverySecond && db.synced.Load() != n
	db.mu.RUnlock()
	if syncDue && syncFile(f) == nil {
		db.ack(n)
	}
	return shrink && db.Shrink() == ErrDatabaseClosed
}

// ack records that the writes up to flush count n are on disk. The value
// only grows, so a slow sync cannot undo a later one.
func (db *DB) ack(n int64) {
	for {
		old := db.synced.Load()
		if old >= n || db.synced.CompareAndSwap(old, n) {
			return
		}
	}
}

// Sync syncs the database file, and then syncs the directory that holds the
// file. Use Sync with the SyncPolicy Never or EverySecond when you need
// durability at a known point.
//
// When Sync returns nil, every write that committed before the call is in the
// file on disk. On Unix, the directory entry of the file is on disk too.
//
// A Sync costs one file sync and then one directory sync. Sync runs them also
// when no commit is unacknowledged. If the file sync fails, Sync skips the
// directory sync. If Close or Shrink closes the file, Sync can make more file
// sync attempts, as the last paragraphs describe. For an in-memory database,
// Sync does nothing and returns nil.
//
// The directory is the real parent of the file. Open resolves it. Shrink
// updates it when it replaces a symlink with the file. The directory sync
// exists on Linux and other Unix systems. On other systems, the directory sync
// does nothing, and Sync syncs the file only. Sync does not sync the ancestors
// of the directory. Those directories must already be durable. Close syncs the
// file, not the directory.
//
// If the file sync succeeds and the directory sync fails, Sync returns the
// directory error. The writes are then in the file on disk, but the directory
// entry of the file can be missing from the disk. The background task does not
// repeat the directory sync. Call Sync again.
//
// Call Sync outside a transaction. Inside Update, Sync deadlocks. Inside View,
// Sync can deadlock when a writer waits. Sync holds no database lock during
// the file sync and the directory sync. A write that commits while the file
// sync runs can be missing from the file on disk.
//
// Sync returns ErrDatabaseClosed if the database is closed before the call, or
// if Close finishes before the file sync starts. If Shrink replaces the file
// before the file sync starts, Sync syncs the new file. If the file sync fails
// because Close or Shrink closed the file, Sync tries again. It makes at most
// three attempts, and then returns the last error. It returns
// ErrDatabaseClosed instead if the database is closed by then. If Close starts
// while the file sync runs, that sync finishes. Sync then returns the result
// of that sync, or the directory error.
//
// The database counts the commits that a sync makes durable. It tracks them
// conservatively, as acknowledged commit counts. Each file sync attempt reads
// the commit count just before it starts. If the sync succeeds, it
// acknowledges that count. That attempt does not acknowledge a write that
// commits after the read. After a retry, the call can acknowledge a write that
// committed during the call. A concurrent Sync can acknowledge a write that
// another call does not.
//
// A successful Shrink and an Always sync acknowledge nothing. The background
// task can then sync once for writes that are already on disk. With SyncPolicy
// EverySecond, the background task syncs the file while a commit is
// unacknowledged. If the file sync of Sync fails, it acknowledges nothing, so
// the background task tries again. If a sync already acknowledged every
// commit, nothing retries a failed Sync.
func (db *DB) Sync() error { return db.syncWith((*os.File).Sync) }

// syncAttempts is the most file fsync attempts that Sync makes when Close or
// Shrink closes the file under it.
const syncAttempts = 3

// syncWith is Sync with the file fsync as a parameter; tests pass one that holds.
func (db *DB) syncWith(syncFile func(*os.File) error) error {
	for attempt := 1; ; attempt++ {
		db.mu.RLock()
		closed, persist, f, dir, n := db.closed, db.persist, db.file, db.dir, db.flushes
		db.mu.RUnlock()
		if closed {
			return ErrDatabaseClosed
		}
		if !persist {
			return nil
		}
		err := syncFile(f)
		if err == nil {
			db.ack(n) // the file is durable, whatever the directory sync does
			return syncDir(dir)
		}
		if !errors.Is(err, os.ErrClosed) {
			return err
		}
		// Close or Shrink closed f before the fsync. Look at the state again.
		if attempt == syncAttempts {
			db.mu.RLock()
			closed = db.closed
			db.mu.RUnlock()
			if closed {
				return ErrDatabaseClosed
			}
			return err
		}
	}
}

// shrinkDue reports whether an aof file of pos bytes is more than pct percent
// larger than last, the size after the previous shrink. A pct of zero or less
// means any growth, as does a last of zero. The arithmetic is exact and cannot overflow.
func shrinkDue(pos, last int64, pct int) bool {
	if pos <= last {
		return false
	}
	if pct <= 0 || last <= 0 {
		return true
	}
	hi, lo := bits.Mul64(uint64(last), uint64(pct))
	if hi >= 100 {
		return false // last*pct/100 exceeds 2^64, so it exceeds any growth
	}
	limit, _ := bits.Div64(hi, lo, 100)
	return uint64(pos-last) > limit
}

// Shrink makes the database file smaller. It writes a snapshot of the items,
// one command for each item, and then appends the commands that commit while
// it runs. It removes the earlier redundant commands.
//
// Shrink does not hold a lock for the whole operation. It reads the items in
// chunks of about 1000 items, and holds the read lock for each chunk. At the
// end, it holds the write lock while it copies the new commands and replaces
// the file.
//
// Shrink writes the new file to a temporary file in the same directory. It
// syncs that file before it renames the file over the old one. The new file
// then has complete contents when it replaces the old file. A directory sync
// then makes the rename durable on Unix. On other systems, that sync does
// nothing. The durability limits of SyncPolicy still apply to commits that no
// sync covered.
//
// The new file keeps the permission bits of the old file. On Unix, it also
// keeps the owner and the group. On Linux, it also keeps the POSIX ACL. If
// Shrink cannot copy the owner or the ACL, the new file gets at most mode 0600
// and no ACL. Shrink does not report this narrowing. If Shrink cannot remove
// an inherited ACL, it returns the error and the database stays open on the
// old file. On Windows, the new file inherits its DACL from the directory.
// Shrink does not copy an explicit DACL from the old file.
//
// If a failed Shrink leaves the database file unusable, Shrink closes the
// database and returns the error. Later calls return ErrDatabaseClosed. Shrink
// does not panic for this reason. If only the final directory sync fails, the
// database stays open and uses the new file.
//
// Shrink returns ErrShrinkInProcess if another Shrink is running. It returns
// ErrDatabaseClosed if the database is closed. For an in-memory database, it
// does nothing and returns nil.
func (db *DB) Shrink() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDatabaseClosed
	}
	if !db.persist {
		// The database was opened with ":memory:" as the path.
		// There is no persistence, and no need to do anything here.
		db.mu.Unlock()
		return nil
	}
	if db.shrinking {
		// The database is already in the process of shrinking.
		db.mu.Unlock()
		return ErrShrinkInProcess
	}
	db.shrinking = true
	defer func() {
		db.mu.Lock()
		db.shrinking = false
		db.mu.Unlock()
	}()
	fname := db.path
	// the endpos is used to return to the end of the file when we are
	// finished writing all of the current items.
	endpos, err := db.file.Seek(0, io.SeekEnd)
	db.mu.Unlock()
	if err != nil {
		return err
	}
	time.Sleep(time.Second / 4) // wait just a bit before starting
	// The temp file is new and private (mode 0600). It is in the database
	// directory so that the final rename stays on one file system.
	f, err := os.CreateTemp(filepath.Dir(fname), filepath.Base(fname)+".*.tmp")
	if err != nil {
		return err
	}
	tmpname := f.Name()
	defer func() {
		_ = f.Close()
		if tmpname != "" { // empty once renamed over the database file
			_ = os.Remove(tmpname)
		}
	}()

	// we are going to read items in as chunks as to not hold up the database
	// for too long.
	var buf []byte
	pivot := ""
	done := false
	for !done {
		err := func() error {
			db.mu.RLock()
			defer db.mu.RUnlock()
			if db.closed {
				return ErrDatabaseClosed
			}
			done = true
			var n int
			btreeAscendGreaterOrEqual(db.keys, &dbItem{key: pivot},
				func(dbi *dbItem) bool {
					// 1000 items or 64MB buffer
					if n > 1000 || len(buf) > 64*1024*1024 {
						pivot = dbi.key
						done = false
						return false
					}
					buf = dbi.writeSetTo(buf)
					n++
					return true
				},
			)
			if len(buf) > 0 {
				if _, err := f.Write(buf); err != nil {
					return err
				}
				buf = buf[:0]
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	// We reached this far so all of the items have been written to a new tmp
	// There's some more work to do by appending the new line from the aof
	// to the tmp file and finally swap the files out.
	return func() error {
		// We're wrapping this in a function to get the benefit of a defered
		// lock/unlock.
		db.mu.Lock()
		defer db.mu.Unlock()
		if db.closed {
			return ErrDatabaseClosed
		}
		// We are going to open a new version of the aof file so that we do
		// not change the seek position of the previous. This may cause a
		// problem in the future if we choose to use syscall file locking.
		aof, err := os.Open(fname)
		if err != nil {
			return err
		}
		defer func() { _ = aof.Close() }()
		if _, err := aof.Seek(endpos, 0); err != nil {
			return err
		}
		// Just copy all of the new commands that have occurred since we
		// started the shrink process.
		if _, err := io.Copy(f, aof); err != nil {
			return err
		}
		// Windows cannot replace a file that another handle has open.
		if err := aof.Close(); err != nil {
			return err
		}
		fi, err := db.file.Stat()
		if err != nil {
			return err
		}
		if err := copyPermissions(f, db.file, fi); err != nil {
			return err
		}
		// The data must be on disk before the rename. Until the rename
		// succeeds, the old file is intact and the database stays usable.
		if err := f.Sync(); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		// From here on db.file is unusable. Each failure either reopens a
		// file at the end, or closes the database.
		if err := db.file.Close(); err != nil {
			return db.reopenOrAbandon(fname, err)
		}
		if err := os.Rename(tmpname, fname); err != nil {
			return db.reopenOrAbandon(fname, err)
		}
		tmpname = ""
		pos, err := db.reopen(fname)
		if err != nil {
			db.abandon()
			return err
		}
		db.lastaofsz = pos
		// The rename is durable only after the directory is synced. The
		// database is already using the new file, so it stays open.
		// Shrink may have replaced a symlink with the file, so the file now
		// lives in the directory of fname.
		db.dir = filepath.Dir(fname)
		return syncDir(db.dir)
	}()
}

// reopen opens path for appending, positions it at the end, and makes it
// db.file. It returns the position. db.mu must be held.
func (db *DB) reopen(path string) (int64, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0) // no O_CREATE: the file must exist
	if err != nil {
		return 0, err
	}
	pos, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		_ = f.Close() // a file with no known position must not be written to
		return 0, err
	}
	db.file = f
	return pos, nil
}

// reopenOrAbandon reopens the unchanged database file after cause. If that
// fails too, it closes the database. It returns cause, joined with any reopen
// error. db.mu must be held.
func (db *DB) reopenOrAbandon(path string, cause error) error {
	if _, err := db.reopen(path); err != nil {
		db.abandon()
		return errors.Join(cause, err)
	}
	return cause
}

// abandon closes the database without a file to close, because the file is
// already closed or was never reopened. db.mu must be held.
func (db *DB) abandon() {
	db.closed = true
	db.keys, db.exps, db.idxs, db.file = nil, nil, nil, nil
}

// copyPermissions gives the new file dst the permission bits of src, where fi
// describes src. It copies the owner, group and ACL first, so that no one
// gains access that src did not grant. dst starts with mode 0600. When it
// cannot copy the owner or the ACL, it removes any ACL that dst inherited from
// its directory, and dst keeps fi's mode&0600. When it cannot remove the ACL,
// it returns the error: dst may carry an ACL that src does not have.
func copyPermissions(dst, src *os.File, fi os.FileInfo) error {
	perm := fi.Mode().Perm()
	if copyOwner(dst, fi) && copyACL(dst, src) == nil {
		return dst.Chmod(perm)
	}
	if err := removeACL(dst); err != nil {
		return err
	}
	return dst.Chmod(perm & 0600)
}

func panicErr(err error) {
	panic(fmt.Errorf("buntdb: %w", err))
}

// minLoadGrowth is the smallest step the load buffer grows by once its current
// capacity is exhausted. It caps how much memory an over-large declared length
// can claim speculatively, while keeping the number of copies low for records
// that really are large.
const minLoadGrowth = 64 * 1024

// maxRetainedBuf is the largest transaction, in serialized bytes, whose commit
// buffer is kept for reuse. The buffer of a transaction up to this size stays
// with the DB between commits, whatever capacity append growth gave it. The
// buffer of a larger one is freed after its commit, so one huge transaction
// does not pin its memory.
const maxRetainedBuf = 64 << 20

// readRESPLine borrows the reader's buffer for ordinary headers, which are
// parsed before the next read. Preserve support for longer headers by copying
// the first fragment before ReadBytes reuses that buffer.
func readRESPLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadSlice('\n')
	if err != bufio.ErrBufferFull {
		return line, err
	}
	prefix := append([]byte(nil), line...)
	rest, err := r.ReadBytes('\n')
	return append(prefix, rest...), err
}

// parseRESPLen parses the numeric portion of a RESP header line such as
// "*3\r\n" or "$12\r\n". The length is untrusted input, so non-digits and
// arithmetic overflow are rejected rather than wrapping to a negative value.
func parseRESPLen(line []byte) (n int, ok bool) {
	if len(line) < 4 || line[len(line)-2] != '\r' {
		return 0, false
	}
	if len(line) == 4 {
		// Single digit, the common case for array counts. No loop.
		d := int(line[1] - '0')
		return d, d <= 9
	}
	for _, c := range line[1 : len(line)-2] {
		// c-'0' wraps for bytes below '0', so one unsigned compare rejects
		// every non-digit.
		d := int(c - '0')
		if d > 9 {
			return 0, false
		}
		// Only a value already at the last-digit boundary can overflow:
		// for n < MaxInt/10, n*10+9 < MaxInt. So the exact check runs once
		// per header at most, and never on the common path.
		if n >= math.MaxInt/10 && n > (math.MaxInt-d)/10 {
			return 0, false
		}
		n = n*10 + d
	}
	return n, true
}

// readGrowing reads exactly need bytes into data, growing it only after the
// current capacity is full so capacity stays tied to the bytes that actually
// arrived rather than to an untrusted declared length. It is the cold path:
// readLoad handles records that fit the reusable buffer inline.
func readGrowing(r io.Reader, data []byte, need int) ([]byte, error) {
	for len(data) < need {
		if len(data) == cap(data) {
			// Grow only after the current buffer is full, so capacity stays
			// within max(minLoadGrowth, 16x the bytes actually read) whatever
			// the header claimed. The large factor keeps copies rare: a 1 MiB
			// record needs two grows, not five. Request no more than remains;
			// slices.Grow may round up the capacity.
			grow := minLoadGrowth
			if cap(data) > minLoadGrowth/15 && cap(data) < math.MaxInt/15 {
				grow = cap(data) * 15
			}
			if grow > need-len(data) {
				grow = need - len(data)
			}
			data = slices.Grow(data, grow)
		}
		// Fill the capacity already on hand in one read.
		want := cap(data) - len(data)
		if want > need-len(data) {
			want = need - len(data)
		}
		base := len(data)
		data = data[:base+want]
		if _, err := io.ReadFull(r, data[base:]); err != nil {
			return data, err
		}
	}
	return data, nil
}

// loadCommand returns the command name as a string literal when it is set, del
// or flushdb in any ASCII letter case, so the loader does not allocate it. Any
// other name is copied, and the loader rejects it.
func loadCommand(b []byte) string {
	switch {
	case len(b) == 3 && b[0]|0x20 == 's' && b[1]|0x20 == 'e' && b[2]|0x20 == 't':
		return "set"
	case len(b) == 3 && b[0]|0x20 == 'd' && b[1]|0x20 == 'e' && b[2]|0x20 == 'l':
		return "del"
	case len(b) == 7 && strings.EqualFold(string(b), "flushdb"):
		return "flushdb"
	}
	return string(b)
}

// readLoad reads from the reader and loads commands into the database.
// modTime is the modified time of the reader. A modTime after time.Now() is
// treated as time.Now(), so a legacy "ex" TTL never grows from a future mtime.
// Returns the offset just past the last complete command, and the error if any.
// NUL padding counts when a complete command follows it. A "*0" command counts.
// Trailing NULs do not count, so the caller may truncate the file to n.
func (db *DB) readLoad(rd io.Reader, modTime time.Time) (n int64, err error) {
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()
	totalSize := int64(0)
	pending := int64(0) // NUL bytes seen since the last complete command
	data := make([]byte, 0, 4096)
	parts := make([]string, 0, 8)
	r := bufio.NewReader(rd)
	for {
		// peek at the first byte. If it's a 'nul' control character then
		// ignore it and move to the next byte.
		c, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return totalSize, err
		}
		if c == 0 {
			// ignore nul control characters; count them once a command follows
			pending++
			continue
		}
		if err := r.UnreadByte(); err != nil {
			return totalSize, err
		}

		// read a single command.
		// first we should read the number of parts that the of the command
		cmdByteSize := int64(0)
		line, err := readRESPLine(r)
		if err != nil {
			return totalSize, err
		}
		if line[0] != '*' {
			return totalSize, ErrInvalid
		}
		cmdByteSize += int64(len(line))

		// convert the string number to and int
		count, ok := parseRESPLen(line)
		if !ok {
			return totalSize, ErrInvalid
		}
		// read each part of the command.
		parts = parts[:0]
		for i := 0; i < count; i++ {
			// read the number of bytes of the part.
			line, err := readRESPLine(r)
			if err != nil {
				return totalSize, err
			}
			if line[0] != '$' {
				return totalSize, ErrInvalid
			}
			cmdByteSize += int64(len(line))
			// convert the string number to and int
			size, ok := parseRESPLen(line)
			if !ok {
				return totalSize, ErrInvalid
			}
			if size > math.MaxInt-2 {
				return totalSize, ErrInvalid
			}
			// Read the part body plus its trailing CRLF. The declared length is
			// untrusted, so the buffer grows only as bytes actually arrive: a
			// short header claiming a huge length must not be able to allocate
			// that much. data is reused across commands, so a file of large
			// records stops reallocating after the first one.
			need := size + 2
			if need <= cap(data) {
				// The reusable buffer already fits: one read, as upstream did.
				data = data[:need]
				if _, err = io.ReadFull(r, data); err != nil {
					return totalSize, err
				}
			} else if data, err = readGrowing(r, data[:0], need); err != nil {
				return totalSize, err
			}
			if data[size] != '\r' || data[size+1] != '\n' {
				return totalSize, ErrInvalid
			}
			// copy string; the command name is a literal, not a copy
			if i == 0 {
				parts = append(parts, loadCommand(data[:size]))
			} else {
				parts = append(parts, string(data[:size]))
			}
			cmdByteSize += int64(size + 2)
		}
		// finished reading the command

		if len(parts) == 0 {
			totalSize += pending + cmdByteSize
			pending = 0
			continue
		}
		// loadCommand already matched the command name, ASCII-only and in any
		// letter case, so a multi-byte look-alike such as "ſet" (U+017F)
		// cannot pass as SET.
		if parts[0] == "set" {
			// SET
			if len(parts) < 3 || len(parts) == 4 || len(parts) > 5 {
				return totalSize, ErrInvalid
			}
			if len(parts) == 5 {
				arg := strings.ToLower(parts[3])
				if arg != "ex" && arg != "ae" {
					return totalSize, ErrInvalid
				}
				ex, err := strconv.ParseInt(parts[4], 10, 64)
				if errors.Is(err, strconv.ErrRange) && arg == "ae" {
					// An older writer stored a pre-1970 time as a uint64.
					// Wrapping it back to int64 recovers the signed value.
					if u, uerr := strconv.ParseUint(parts[4], 10, 64); uerr == nil {
						ex, err = int64(u), nil
					}
				}
				if err != nil {
					return totalSize, ErrInvalid
				}
				// time.Unix wraps a larger "ae" to a past time. Smaller values
				// cannot wrap, and are only ever expired.
				if arg == "ae" && ex > maxTime.Unix() {
					return totalSize, ErrInvalid
				}
				var exat time.Time
				now := time.Now()
				if arg == "ex" {
					exat = legacyExpiry(ex, modTime, now)
				} else {
					exat = time.Unix(ex, 0)
				}
				if exat.After(now) {
					db.insertIntoDatabase(&dbItem{
						key: parts[1],
						val: parts[2],
						opts: &dbItemOpts{
							ex:   true,
							exat: exat,
						},
					})
				} else {
					db.deleteFromDatabase(&dbItem{
						key: parts[1],
					})
				}
			} else {
				db.insertIntoDatabase(&dbItem{key: parts[1], val: parts[2]})
			}
		} else if parts[0] == "del" {
			// DEL
			if len(parts) != 2 {
				return totalSize, ErrInvalid
			}
			db.deleteFromDatabase(&dbItem{key: parts[1]})
		} else if parts[0] == "flushdb" {
			db.initTrees()
		} else {
			return totalSize, ErrInvalid
		}
		totalSize += pending + cmdByteSize
		pending = 0
	}
}

// legacyExpiry returns the deadline of a legacy relative "ex" TTL of ex seconds
// in a file last modified at modTime. A deadline that is not after now means
// the item has expired. The arithmetic saturates, so it cannot wrap.
func legacyExpiry(ex int64, modTime, now time.Time) time.Time {
	// A modTime in the future would make the age negative and the sum overflow.
	age := time.Duration(0)
	if modTime.Before(now) {
		age = now.Sub(modTime) // saturates at the largest Duration
	}
	if ex > math.MaxInt64/int64(time.Second) {
		return maxTime // the TTL is longer than any Duration: never expires
	}
	if ex < math.MinInt64/int64(time.Second) {
		return now // the TTL is longer than any Duration: long expired
	}
	d := time.Duration(ex) * time.Second
	if d < math.MinInt64+age {
		return now // d - age would underflow: long expired
	}
	return now.Add(d - age)
}

// load reads entries from the append only database file and fills the database.
// The file format uses the Redis append only file format, which is and a series
// of RESP commands. For more information on RESP please read
// http://redis.io/topics/protocol. The only supported RESP commands are DEL and
// SET.
func (db *DB) load() error {
	fi, err := db.file.Stat()
	if err != nil {
		return err
	}
	n, err := db.readLoad(db.file, fi.ModTime())
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			// The db file has ended mid-command, which is allowed but the
			// data file should be truncated to the end of the last valid
			// command
			if err := db.file.Truncate(n); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	if _, err := db.file.Seek(n, 0); err != nil {
		return err
	}
	var estaofsz int64
	for v := range db.keys.All() {
		estaofsz += int64(v.estAOFSetSize())
	}
	db.lastaofsz += estaofsz
	return nil
}

// managed calls a block of code that is fully contained in a transaction.
// This method is intended to be wrapped by Update and View
func (db *DB) managed(writable bool, fn func(tx *Tx) error) (err error) {
	var tx *Tx
	tx, err = db.Begin(writable)
	if err != nil {
		return
	}
	// Track normal completion so both panic and runtime.Goexit roll back
	// without recovering the callback's panic.
	var returned bool
	defer func() {
		if !returned || err != nil {
			// The callback returned an error, panicked, or exited the
			// goroutine. An incomplete transaction must not be committed.
			_ = tx.Rollback()
			return
		}
		if writable {
			// Everything went well. Lets Commit()
			err = tx.Commit()
		} else {
			// read-only transaction can only roll back.
			err = tx.Rollback()
		}
	}()
	tx.funcd = true
	defer func() {
		tx.funcd = false
	}()
	err = fn(tx)
	returned = true
	return
}

// View runs fn in a managed read-only transaction. View returns the error that
// fn returns.
//
// If fn panics or calls runtime.Goexit, View rolls back the transaction. A
// panic continues to propagate to the caller.
//
// A manual commit or rollback inside fn causes a panic.
func (db *DB) View(fn func(tx *Tx) error) error {
	return db.managed(false, fn)
}

// Update runs fn in a managed read/write transaction. If fn returns an error,
// Update rolls back the transaction and returns that error. If fn returns nil,
// Update commits the transaction and returns any error from Commit.
//
// Under SyncPolicy Always, a sync error leaves the changes applied with
// durability unconfirmed. It does not mean that the transaction rolled back.
// See Commit before you retry a non-idempotent operation after an error.
//
// If fn panics or calls runtime.Goexit, Update rolls back the transaction and
// discards its writes. A panic continues to propagate to the caller.
//
// A manual commit or rollback inside fn causes a panic.
func (db *DB) Update(fn func(tx *Tx) error) error {
	return db.managed(true, fn)
}

// get return an item or nil if not found.
func (db *DB) get(key string) *dbItem {
	return db.keys.Get(&dbItem{key: key})
}

// Tx is a transaction on the database. A transaction is read-only or
// read/write. A read-only transaction gets values and iterates over keys and
// values. A read/write transaction also sets and deletes items.
//
// End each read/write transaction with Commit or Rollback. End each read-only
// transaction with Rollback. The transactions that View and Update run end
// automatically.
type Tx struct {
	db       *DB             // the underlying database.
	writable bool            // when false mutable operations fail.
	funcd    bool            // when true Commit and Rollback panic.
	wc       *txWriteContext // context for writable transactions.
}

type txWriteContext struct {
	// rollback when deleteAll is called
	rbkeys *dbTree           // a tree of all item ordered by key
	rbexps *dbTree           // a tree of items ordered by expiration
	rbidxs map[string]*index // the index trees.

	rollbackItems   map[string]*dbItem // details for rolling back tx.
	commitItems     map[string]*dbItem // details for committing tx.
	itercount       int                // stack of iterators
	rollbackIndexes map[string]*index  // details for dropped indexes.
}

// DeleteAll deletes all items from the database. The indexes stay, and they
// become empty. A rollback of the transaction restores the items.
//
// DeleteAll returns ErrTxClosed if the transaction ended. It returns
// ErrTxNotWritable in a read-only transaction. It returns ErrTxIterating
// inside an iteration callback.
func (tx *Tx) DeleteAll() error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}

	// check to see if we've already deleted everything
	if tx.wc.rbkeys == nil {
		// we need to backup the live data in case of a rollback.
		tx.wc.rbkeys = tx.db.keys
		tx.wc.rbexps = tx.db.exps
		tx.wc.rbidxs = tx.db.idxs
	}

	// now reset the live database trees
	idxs := tx.db.idxs
	tx.db.keys = btreeNew(lessByKey)
	tx.db.exps = btreeNew(lessExps)
	tx.db.idxs = make(map[string]*index, len(idxs))

	// finally re-create the current indexes, not the ones saved for rollback
	for name, idx := range idxs {
		tx.db.idxs[name] = idx.clearCopy()
	}

	// always clear out the commits
	tx.wc.commitItems = make(map[string]*dbItem)

	return nil
}

// Begin starts a new transaction. If writable is true, the transaction is
// read/write. If writable is false, it is read-only.
//
// Many read-only transactions can run at the same time. Only one read/write
// transaction can run at a time. A read/write transaction waits until all
// other transactions end.
//
// Begin returns ErrDatabaseClosed if the database is closed. End each
// read/write transaction with Commit or Rollback. End each read-only
// transaction with Rollback. View and Update do this for you.
func (db *DB) Begin(writable bool) (*Tx, error) {
	tx := &Tx{
		db:       db,
		writable: writable,
	}
	tx.lock()
	if db.closed {
		tx.unlock()
		return nil, ErrDatabaseClosed
	}
	if writable {
		// writable transactions have a writeContext object that
		// contains information about changes to the database.
		tx.wc = &txWriteContext{}
		tx.wc.rollbackItems = make(map[string]*dbItem)
		// rollbackIndexes is created by the first CreateIndex or DropIndex;
		// most transactions never touch an index definition.
		if db.persist {
			tx.wc.commitItems = make(map[string]*dbItem)
		}
	}
	return tx, nil
}

// lock locks the database based on the transaction type.
func (tx *Tx) lock() {
	if tx.writable {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}

// unlock unlocks the database based on the transaction type.
func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}

// rollbackInner handles the underlying rollback logic.
// Intended to be called from Commit() and Rollback().
func (tx *Tx) rollbackInner() {
	// rollback the deleteAll if needed
	if tx.wc.rbkeys != nil {
		tx.db.keys = tx.wc.rbkeys
		tx.db.idxs = tx.wc.rbidxs
		tx.db.exps = tx.wc.rbexps
	}
	for key, item := range tx.wc.rollbackItems {
		tx.db.deleteFromDatabase(&dbItem{key: key})
		if item != nil {
			// When an item is not nil, we will need to reinsert that item
			// into the database overwriting the current one.
			tx.db.insertIntoDatabase(item)
		}
	}
	for name, idx := range tx.wc.rollbackIndexes {
		delete(tx.db.idxs, name)
		if idx != nil {
			// When an index is not nil, we will need to rebuilt that index
			// this could be an expensive process if the database has many
			// items or the index is complex.
			tx.db.idxs[name] = idx
			idx.rebuild()
		}
	}
}

// Commit ends a read/write transaction. For a database that persists to disk,
// it appends the item changes of the transaction to the database file. It does
// not write index definitions. It returns ErrTxNotWritable for a read-only
// transaction, and it does not end that transaction. Use Rollback to end a
// read-only transaction. It returns ErrTxClosed if the transaction already
// ended. Commit panics inside a function that View or Update runs.
//
// If the write fails, Commit rolls back the changes and returns the write
// error. If a failed write leaves partial data in the file and Commit cannot
// remove it, Commit panics.
//
// Under SyncPolicy Always, Commit also syncs the file, if the commit writes to
// the file. If that sync fails, Commit returns the sync error. Commit appends
// the data, and the changes stay in memory. The error means that durability is
// unconfirmed. It does not mean that the transaction rolled back.
//
// Do not treat a sync error as a rollback. A retry of a non-idempotent
// operation applies it twice. A later sync error never replaces a write error.
// After a sync error, later operations still run. A failed sync does not show
// that the storage is healthy.
func (tx *Tx) Commit() error {
	if tx.funcd {
		panic("managed tx commit not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}
	var err error
	if tx.db.persist && (len(tx.wc.commitItems) > 0 || tx.wc.rbkeys != nil) {
		tx.db.buf = tx.db.buf[:0]
		// write a flushdb if a deleteAll was called.
		if tx.wc.rbkeys != nil {
			tx.db.buf = append(tx.db.buf, "*1\r\n$7\r\nflushdb\r\n"...)
		}
		// Each committed record is written to disk
		for key, item := range tx.wc.commitItems {
			if item == nil {
				tx.db.buf = (&dbItem{key: key}).writeDeleteTo(tx.db.buf)
			} else {
				tx.db.buf = item.writeSetTo(tx.db.buf)
			}
		}
		// Flushing the buffer only once per transaction.
		// If this operation fails then the write did failed and we must
		// rollback.
		var n int
		n, err = tx.db.file.Write(tx.db.buf)
		if len(tx.db.buf) > maxRetainedBuf { // this commit's serialized size
			tx.db.buf = nil
		}
		if err != nil {
			if n > 0 {
				// There was a partial write to disk.
				// We are possibly out of disk space.
				// Delete the partially written bytes from the data file by
				// seeking to the previously known position and performing
				// a truncate operation.
				// At this point a syscall failure is fatal and the process
				// should be killed to avoid corrupting the file.
				pos, err := tx.db.file.Seek(-int64(n), 1)
				if err != nil {
					panicErr(err)
				}
				if err := tx.db.file.Truncate(pos); err != nil {
					panicErr(err)
				}
			}
			tx.rollbackInner()
		}
		if tx.db.config.SyncPolicy == Always {
			// Preserve an append error, whose changes were rolled back above.
			// After a successful append, a sync failure leaves the changes
			// applied but their durability unconfirmed.
			if serr := tx.db.file.Sync(); serr != nil && err == nil {
				err = serr
			}
		}
		// Increment the number of flushes. The background syncing uses this.
		tx.db.flushes++
	}
	// Unlock the database and allow for another writable transaction.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return err
}

// Rollback ends the transaction and reverts all changes that it made, such as
// Set and Delete. A read-only transaction can only roll back. Rollback returns
// ErrTxClosed if the transaction already ended. It panics inside a function
// that View or Update runs.
func (tx *Tx) Rollback() error {
	if tx.funcd {
		panic("managed tx rollback not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	}
	// The rollback func does the heavy lifting.
	if tx.writable {
		tx.rollbackInner()
	}
	// unlock the database for more transactions.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return nil
}

// dbItemOpts holds various meta information about an item.
type dbItemOpts struct {
	ex   bool      // does this item expire?
	exat time.Time // when does this item expire?
}
type dbItem struct {
	key, val string      // the binary key and value
	opts     *dbItemOpts // optional meta information
	keyless  bool        // keyless item for scanning
}

// estIntSize returns the string representions size.
// Has the same result as len(strconv.Itoa(x)).
func estIntSize(x int) int {
	n := 1
	if x < 0 {
		n++
		x *= -1
	}
	for x >= 10 {
		n++
		x /= 10
	}
	return n
}

func estArraySize(count int) int {
	return 1 + estIntSize(count) + 2
}

func estBulkStringSize(s string) int {
	return 1 + estIntSize(len(s)) + 2 + len(s) + 2
}

// estBulkInt64Size returns the size of n written by appendBulkInt: the signed
// decimal digits as a bulk string. It does not allocate.
func estBulkInt64Size(n int64) int {
	u := uint64(n)
	digits := 1
	if n < 0 {
		u = -u   // wraps correctly for math.MinInt64
		digits++ // the sign
	}
	for ; u >= 10; u /= 10 {
		digits++
	}
	return 1 + estIntSize(digits) + 2 + digits + 2
}

// estAOFSetSize returns the number of bytes writeSetTo appends for this item.
func (dbi *dbItem) estAOFSetSize() int {
	var n int
	if dbi.opts != nil && dbi.opts.ex {
		n += estArraySize(5)
		n += estBulkStringSize("set")
		n += estBulkStringSize(dbi.key)
		n += estBulkStringSize(dbi.val)
		n += estBulkStringSize("ae")
		n += estBulkInt64Size(dbi.opts.exat.Unix())
	} else {
		n += estArraySize(3)
		n += estBulkStringSize("set")
		n += estBulkStringSize(dbi.key)
		n += estBulkStringSize(dbi.val)
	}
	return n
}

func appendArray(buf []byte, count int) []byte {
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(count), 10)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendBulkString(buf []byte, s string) []byte {
	buf = append(buf, '$')
	buf = strconv.AppendInt(buf, int64(len(s)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}

// appendBulkInt appends n as a bulk string. It allocates only if buf lacks capacity.
func appendBulkInt(buf []byte, n int64) []byte {
	var digits [20]byte // fits the longest int64, "-9223372036854775808"
	num := strconv.AppendInt(digits[:0], n, 10)
	buf = append(buf, '$')
	buf = strconv.AppendInt(buf, int64(len(num)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, num...)
	buf = append(buf, '\r', '\n')
	return buf
}

// writeSetTo appends the item as a single SET record to buf and returns it.
func (dbi *dbItem) writeSetTo(buf []byte) []byte {
	if dbi.opts != nil && dbi.opts.ex {
		buf = appendArray(buf, 5)
		buf = appendBulkString(buf, "set")
		buf = appendBulkString(buf, dbi.key)
		buf = appendBulkString(buf, dbi.val)
		buf = appendBulkString(buf, "ae")
		buf = appendBulkInt(buf, dbi.opts.exat.Unix())
	} else {
		buf = appendArray(buf, 3)
		buf = appendBulkString(buf, "set")
		buf = appendBulkString(buf, dbi.key)
		buf = appendBulkString(buf, dbi.val)
	}
	return buf
}

// writeDeleteTo appends the item's key as a single DEL record to buf and returns it.
func (dbi *dbItem) writeDeleteTo(buf []byte) []byte {
	buf = appendArray(buf, 2)
	buf = appendBulkString(buf, "del")
	buf = appendBulkString(buf, dbi.key)
	return buf
}

// expired evaluates id the item has expired. This will always return false when
// the item does not have `opts.ex` set to true.
func (dbi *dbItem) expired() bool {
	// Kept small enough to inline: the common case is an item with no
	// expiry, which must not pay a call per scanned item. The clock read is
	// in expiredNow and runs only for items that can expire.
	return dbi.opts != nil && dbi.opts.expiredNow()
}

// expiredNow reports whether an expiring item's deadline has passed.
func (o *dbItemOpts) expiredNow() bool {
	return o.ex && time.Now().After(o.exat)
}

// expiredAt reports whether an expiring item's deadline has passed at now.
func (o *dbItemOpts) expiredAt(now time.Time) bool {
	return o.ex && now.After(o.exat)
}

// lazyClock gives one clock reading to a whole traversal. It reads the clock
// on the first item that can expire, so a traversal without TTL items never
// reads it.
type lazyClock struct {
	now time.Time
}

// past reports whether o's deadline has passed at the single clock reading.
func (c *lazyClock) past(o *dbItemOpts) bool {
	if c.now.IsZero() {
		c.now = time.Now()
	}
	return o.expiredAt(c.now)
}

// expired reports whether dbi has expired as of the clock's single reading.
// The check for an item without a TTL inlines, so it costs no call.
func (c *lazyClock) expired(dbi *dbItem) bool {
	return dbi.opts != nil && dbi.opts.ex && c.past(dbi.opts)
}

// MaxTime from http://stackoverflow.com/questions/25065055#32620397
// This is a long time in the future. It's an imaginary number that is
// used for b-tree ordering.
var maxTime = time.Unix(1<<63-62135596801, 999999999)

// expiresAt will return the time when the item will expire. When an item does
// not expire `maxTime` is used.
func (dbi *dbItem) expiresAt() time.Time {
	if dbi.opts == nil || !dbi.opts.ex {
		return maxTime
	}
	return dbi.opts.exat
}

// lessByKey orders items by key. It is the keys tree's comparator and the
// final tie-break for every other tree, which is what keeps each item unique
// within an index. A keyless item sorts after every real item with the same
// value, so a {val: x, keyless: true} pivot lands past the end of x's run.
func lessByKey(a, b *dbItem) bool {
	if a.keyless {
		return false
	} else if b.keyless {
		return true
	}
	return a.key < b.key
}

// lessExps orders the expiration tree: soonest expiry first, then by key.
func lessExps(a, b *dbItem) bool {
	ea, eb := a.expiresAt(), b.expiresAt()
	if eb.After(ea) {
		return true
	}
	if ea.After(eb) {
		return false
	}
	return lessByKey(a, b)
}

// lessItems is the comparator for this index's b-tree: the index's less
// function on values, then key as the tie-break.
func (idx *index) lessItems(a, b *dbItem) bool {
	if idx.less != nil {
		if idx.less(a.val, b.val) {
			return true
		}
		if idx.less(b.val, a.val) {
			return false
		}
	}
	return lessByKey(a, b)
}

// Rect returns the rectangle of the item, as the rect function of the index
// ctx computes it. It returns nil slices for any other ctx.
func (dbi *dbItem) Rect(ctx any) (min, max []float64) {
	switch ctx := ctx.(type) {
	case *index:
		return ctx.rect(dbi.val)
	}
	return nil, nil
}

// SetOptions holds the options of Set.
type SetOptions struct {
	// Expires makes the item expire after TTL.
	Expires bool
	// TTL is the time that the item stays in the database before it expires. TTL
	// has an effect only when Expires is true. TTL stands for time to live. After
	// the TTL, Get and the iterators skip the item. Get with ignoreExpired set to
	// true still returns it. The background task deletes it, unless an OnExpired
	// or OnExpiredSync callback in Config handles it.
	TTL time.Duration
}

// GetLess returns the less function of an index. Use it to compare two values
// inside a transaction. It returns ErrNotFound if the index does not exist or
// has no less function. It returns ErrTxClosed if the transaction ended.
func (tx *Tx) GetLess(index string) (func(a, b string) bool, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	idx, ok := tx.db.idxs[index]
	if !ok || idx.less == nil {
		return nil, ErrNotFound
	}
	return idx.less, nil
}

// GetRect returns the rect function of an index. Use it to convert a string to
// a rectangle inside a transaction. It returns ErrNotFound if the index does
// not exist or has no rect function. It returns ErrTxClosed if the transaction
// ended.
func (tx *Tx) GetRect(index string) (func(s string) (min, max []float64),
	error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	idx, ok := tx.db.idxs[index]
	if !ok || idx.rect == nil {
		return nil, ErrNotFound
	}
	return idx.rect, nil
}

// Set inserts an item, or replaces the item with the same key. The opts
// argument sets extra options, such as an expiration time. It can be nil.
//
// Set returns err as nil on success. Set returns replaced as true if it
// replaces an existing item. In that case, previousValue holds the value of
// the replaced item. Set does not count an expired item as replaced.
//
// Other transactions do not see the change until this transaction commits.
//
// Set needs a read/write transaction. In a read-only transaction, Set returns
// ErrTxNotWritable. Inside an iteration callback, such as Ascend, Descend,
// Intersects or Nearby, Set returns ErrTxIterating.
func (tx *Tx) Set(key, value string, opts *SetOptions) (previousValue string,
	replaced bool, err error) {
	if tx.db == nil {
		return "", false, ErrTxClosed
	} else if !tx.writable {
		return "", false, ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return "", false, ErrTxIterating
	}
	item := &dbItem{key: key, val: value}
	if opts != nil {
		if opts.Expires {
			// The caller is requesting that this item expires. Convert the
			// TTL to an absolute time and bind it to the item.
			item.opts = &dbItemOpts{ex: true, exat: time.Now().Add(opts.TTL)}
		}
	}
	// Insert the item into the keys tree.
	prev := tx.db.insertIntoDatabase(item)

	// An expired previous item does not count as replaced.
	if prev != nil && !prev.expired() {
		previousValue, replaced = prev.val, true
	}
	// insert into the rollback map if there has not been a deleteAll.
	if tx.wc.rbkeys == nil {
		// A nil prev marks the key for deletion on rollback. A non-nil prev
		// restores that item. The first entry for a key takes precedence.
		if _, ok := tx.wc.rollbackItems[key]; !ok {
			tx.wc.rollbackItems[key] = prev
		}
	}
	// For commits we simply assign the item to the map. We use this map to
	// write the entry to disk.
	if tx.db.persist {
		tx.wc.commitItems[key] = item
	}
	return previousValue, replaced, nil
}

// Get returns the value of a key. It returns ErrNotFound if the item does not
// exist or has expired. If ignoreExpired is true, Get returns the value of an
// expired item too.
func (tx *Tx) Get(key string, ignoreExpired ...bool) (val string, err error) {
	if tx.db == nil {
		return "", ErrTxClosed
	}
	var ignore bool
	if len(ignoreExpired) != 0 {
		ignore = ignoreExpired[0]
	}
	item := tx.db.get(key)
	if item == nil || (item.expired() && !ignore) {
		// The item does not exists or has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return "", ErrNotFound
	}
	return item.val, nil
}

// Delete removes the item with the given key and returns its value. It returns
// ErrNotFound if the item does not exist or has expired. Delete removes an
// expired item too, but it still returns ErrNotFound.
//
// Delete needs a read/write transaction. In a read-only transaction, Delete
// returns ErrTxNotWritable. Inside an iteration callback, Delete returns
// ErrTxIterating.
func (tx *Tx) Delete(key string) (val string, err error) {
	if tx.db == nil {
		return "", ErrTxClosed
	} else if !tx.writable {
		return "", ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return "", ErrTxIterating
	}
	item := tx.db.deleteFromDatabase(&dbItem{key: key})
	if item == nil {
		return "", ErrNotFound
	}
	// create a rollback entry if there has not been a deleteAll call.
	if tx.wc.rbkeys == nil {
		if _, ok := tx.wc.rollbackItems[key]; !ok {
			tx.wc.rollbackItems[key] = item
		}
	}
	if tx.db.persist {
		tx.wc.commitItems[key] = nil
	}
	// Even though the item has been deleted, we still want to check
	// if it has expired. An expired item should not be returned.
	if item.expired() {
		// The item exists in the tree, but has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return "", ErrNotFound
	}
	return item.val, nil
}

// TTL returns the remaining time to live of an item. It returns a duration of
// -1 for an item that does not expire. It returns ErrNotFound if the item does
// not exist or has expired.
func (tx *Tx) TTL(key string) (time.Duration, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	item := tx.db.get(key)
	if item == nil {
		return 0, ErrNotFound
	} else if item.opts == nil || !item.opts.ex {
		return -1, nil
	}
	dur := time.Until(item.opts.exat)
	if dur < 0 {
		return 0, ErrNotFound
	}
	return dur, nil
}

// scan iterates through a specified index and calls user-defined iterator
// function for each item encountered.
// The desc param indicates that the iterator should descend.
// The gt param indicates that there is a greaterThan limit.
// The lt param indicates that there is a lessThan limit.
// The index param tells the scanner to use the specified index tree. An
// empty string for the index means to scan the keys, not the values.
// The start and stop params are the greaterThan, lessThan limits. For
// descending order, these will be lessThan, greaterThan.
// An error will be returned if the tx is closed or the index is not found.
func (tx *Tx) scan(desc, gt, lt bool, index, start, stop string,
	iterator func(key, value string) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	// wrap a btree specific iterator around the user-defined iterator.
	iter := func(dbi *dbItem) bool {
		if dbi.expired() {
			return true
		}
		return iterator(dbi.key, dbi.val)
	}
	var tr *dbTree
	if index == "" {
		// empty index means we will use the keys tree.
		tr = tx.db.keys
	} else {
		idx := tx.db.idxs[index]
		if idx == nil {
			// index was not found. return error
			return ErrNotFound
		}
		tr = idx.btr
		if tr == nil {
			return nil
		}
	}
	// create some limit items
	var itemA, itemB *dbItem
	if gt || lt {
		if index == "" {
			itemA = &dbItem{key: start}
			itemB = &dbItem{key: stop}
		} else {
			itemA = &dbItem{val: start}
			itemB = &dbItem{val: stop}
			if desc {
				itemA.keyless = true
				itemB.keyless = true
			}
		}
	}
	// execute the scan on the underlying tree.
	if tx.wc != nil {
		tx.wc.itercount++
		defer func() {
			tx.wc.itercount--
		}()
	}
	if desc {
		if gt {
			if lt {
				btreeDescendRange(tr, itemA, itemB, iter)
			} else {
				btreeDescendGreaterThan(tr, itemA, iter)
			}
		} else if lt {
			btreeDescendLessOrEqual(tr, itemA, iter)
		} else {
			btreeDescend(tr, iter)
		}
	} else {
		if gt {
			if lt {
				btreeAscendRange(tr, itemA, itemB, iter)
			} else {
				btreeAscendGreaterOrEqual(tr, itemA, iter)
			}
		} else if lt {
			btreeAscendLessThan(tr, itemA, iter)
		} else {
			btreeAscend(tr, iter)
		}
	}
	return nil
}

// Match returns true if key matches pattern. In the pattern, '*' matches any
// number of characters, and '?' matches any one character.
func Match(key, pattern string) bool {
	return match.Match(key, pattern)
}

// AscendKeys calls iterator for every item whose key matches pattern, in
// ascending key order, until iterator returns false. It skips expired items.
// An empty pattern matches no key, and AscendKeys returns nil.
func (tx *Tx) AscendKeys(pattern string,
	iterator func(key, value string) bool) error {
	if pattern == "" {
		return nil
	}
	if pattern[0] == '*' {
		if pattern == "*" {
			return tx.Ascend("", iterator)
		}
		return tx.Ascend("", func(key, value string) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return tx.AscendGreaterOrEqual("", min, func(key, value string) bool {
		if key > max {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// DescendKeys calls iterator for every item whose key matches pattern, in
// descending key order, until iterator returns false. It skips expired items.
// An empty pattern matches no key, and DescendKeys returns nil.
func (tx *Tx) DescendKeys(pattern string,
	iterator func(key, value string) bool) error {
	if pattern == "" {
		return nil
	}
	if pattern[0] == '*' {
		if pattern == "*" {
			return tx.Descend("", iterator)
		}
		return tx.Descend("", func(key, value string) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return tx.DescendLessOrEqual("", max, func(key, value string) bool {
		if key < min {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// Ascend calls iterator for every item in the database, from first to last,
// until iterator returns false.
//
// If index is not empty, the order follows the less function of that index. If
// index is empty, the order follows the key. The function skips expired items.
// It returns ErrNotFound if the index does not exist.
func (tx *Tx) Ascend(index string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, false, false, index, "", "", iterator)
}

// AscendGreaterOrEqual calls iterator for every item in the range [pivot,
// last], until iterator returns false.
//
// If index is empty, the order follows the key, and the pivot is a key. If
// index is not empty, the order follows the less function of that index, and
// the pivot is a value that the function compares. The function skips expired
// items. It returns ErrNotFound if the index does not exist.
func (tx *Tx) AscendGreaterOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, true, false, index, pivot, "", iterator)
}

// AscendLessThan calls iterator for every item in the range [first, pivot),
// until iterator returns false.
//
// If index is empty, the order follows the key, and the pivot is a key. If
// index is not empty, the order follows the less function of that index, and
// the pivot is a value that the function compares. The function skips expired
// items. It returns ErrNotFound if the index does not exist.
func (tx *Tx) AscendLessThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, false, true, index, pivot, "", iterator)
}

// AscendRange calls iterator for every item in the range [greaterOrEqual,
// lessThan), until iterator returns false.
//
// If index is empty, the order follows the key, and the bounds are keys. If
// index is not empty, the order follows the less function of that index, and
// the bounds are values that the function compares. The function skips expired
// items. It returns ErrNotFound if the index does not exist.
func (tx *Tx) AscendRange(index, greaterOrEqual, lessThan string,
	iterator func(key, value string) bool) error {
	return tx.scan(
		false, true, true, index, greaterOrEqual, lessThan, iterator,
	)
}

// Descend calls iterator for every item in the database, from last to first,
// until iterator returns false.
//
// If index is not empty, the order follows the less function of that index. If
// index is empty, the order follows the key. The function skips expired items.
// It returns ErrNotFound if the index does not exist.
func (tx *Tx) Descend(index string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, false, false, index, "", "", iterator)
}

// DescendGreaterThan calls iterator for every item in the range [last, pivot),
// until iterator returns false. The items are greater than pivot.
//
// If index is empty, the order follows the key, and the pivot is a key. If
// index is not empty, the order follows the less function of that index, and
// the pivot is a value that the function compares. The function skips expired
// items. It returns ErrNotFound if the index does not exist.
func (tx *Tx) DescendGreaterThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, true, false, index, pivot, "", iterator)
}

// DescendLessOrEqual calls iterator for every item in the range [pivot,
// first], until iterator returns false.
//
// If index is empty, the order follows the key, and the pivot is a key. If
// index is not empty, the order follows the less function of that index, and
// the pivot is a value that the function compares. The function skips expired
// items. It returns ErrNotFound if the index does not exist.
func (tx *Tx) DescendLessOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, false, true, index, pivot, "", iterator)
}

// DescendRange calls iterator for every item in the range [lessOrEqual,
// greaterThan), until iterator returns false. The items are less than or equal
// to lessOrEqual, and greater than greaterThan.
//
// If index is empty, the order follows the key, and the bounds are keys. If
// index is not empty, the order follows the less function of that index, and
// the bounds are values that the function compares. The function skips expired
// items. It returns ErrNotFound if the index does not exist.
func (tx *Tx) DescendRange(index, lessOrEqual, greaterThan string,
	iterator func(key, value string) bool) error {
	return tx.scan(
		true, true, true, index, lessOrEqual, greaterThan, iterator,
	)
}

// AscendEqual calls iterator for every item that equals pivot, in ascending
// order, until iterator returns false.
//
// If index is empty, the order follows the key, and the pivot is a key. If
// index is not empty, the order follows the less function of that index, and
// the pivot is a value that the function compares. The function skips expired
// items. It returns ErrNotFound if the index does not exist or has no less
// function.
func (tx *Tx) AscendEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	var err error
	var less func(a, b string) bool
	if index != "" {
		less, err = tx.GetLess(index)
		if err != nil {
			return err
		}
	}
	return tx.AscendGreaterOrEqual(index, pivot, func(key, value string) bool {
		if less == nil {
			if key != pivot {
				return false
			}
		} else if less(pivot, value) {
			return false
		}
		return iterator(key, value)
	})
}

// DescendEqual calls iterator for every item that equals pivot, in descending
// order, until iterator returns false.
//
// If index is empty, the order follows the key, and the pivot is a key. If
// index is not empty, the order follows the less function of that index, and
// the pivot is a value that the function compares. The function skips expired
// items. It returns ErrNotFound if the index does not exist or has no less
// function.
func (tx *Tx) DescendEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	var err error
	var less func(a, b string) bool
	if index != "" {
		less, err = tx.GetLess(index)
		if err != nil {
			return err
		}
	}
	return tx.DescendLessOrEqual(index, pivot, func(key, value string) bool {
		if less == nil {
			if key != pivot {
				return false
			}
		} else if less(value, pivot) {
			return false
		}
		return iterator(key, value)
	})
}

// rect is used by Intersects and Nearby
type rect struct {
	min, max []float64
}

func (r *rect) Rect(ctx any) (min, max []float64) {
	return r.min, r.max
}

// Nearby searches a spatial index for items near a target rectangle. It calls
// iterator for every item, from nearest to farthest, until iterator returns
// false.
//
// The bounds string describes the target. Nearby converts it with the rect
// function that CreateSpatialIndex received. The dist argument is the squared
// distance between the bounding boxes. For two 2-D points, it is the squared
// distance between the points.
//
// Nearby returns ErrNotFound if the index does not exist. If index is empty or
// is not a spatial index, Nearby returns nil and does not call iterator.
//
// Nearby skips expired items, as Get does. It checks expiration against one
// clock reading for each call.
//
// A write inside the iterator returns ErrTxIterating in a read/write
// transaction. In a read-only transaction, it returns ErrTxNotWritable.
//
// Nearby sorts by distance only among items with the same number of
// dimensions. An index that holds rectangles with different numbers of
// dimensions can return items out of order.
//
// In such an index, an iterator that panics or calls runtime.Goexit can leak
// goroutines. testing.T.Fatal calls runtime.Goexit. Keep fatal assertions out
// of the iterator.
//
// In such an index, background goroutines can remain after the iterator
// returns false. Do not rely on Nearby to finish before a following write.
func (tx *Tx) Nearby(index, bounds string,
	iterator func(key, value string, dist float64) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	if index == "" {
		// cannot search on keys tree. just return nil.
		return nil
	}
	// wrap a rtree specific iterator around the user-defined iterator.
	var clock lazyClock
	iter := func(item rtred.Item, dist float64) bool {
		dbi := item.(*dbItem)
		if clock.expired(dbi) {
			return true
		}
		return iterator(dbi.key, dbi.val, dist)
	}
	idx := tx.db.idxs[index]
	if idx == nil {
		// index was not found. return error
		return ErrNotFound
	}
	if idx.rtr == nil {
		// not an r-tree index. just return nil
		return nil
	}
	// execute the nearby search
	var min, max []float64
	if idx.rect != nil {
		min, max = idx.rect(bounds)
	}
	if tx.wc != nil {
		tx.wc.itercount++
		defer func() {
			tx.wc.itercount--
		}()
	}
	// set the center param to false, which uses the box dist calc.
	idx.rtr.KNN(&rect{min, max}, false, iter)
	return nil
}

// Intersects searches a spatial index for items that intersect a target
// rectangle. It calls iterator for every such item until iterator returns
// false.
//
// The bounds string describes the target. Intersects converts it with the rect
// function that CreateSpatialIndex received.
//
// Intersects returns ErrNotFound if the index does not exist. If index is
// empty or is not a spatial index, Intersects returns nil and does not call
// iterator.
//
// Intersects skips expired items, as Get does. It checks expiration against
// one clock reading for each call. The search stops when the iterator returns
// false.
//
// A write inside the iterator returns ErrTxIterating in a read/write
// transaction. In a read-only transaction, it returns ErrTxNotWritable.
func (tx *Tx) Intersects(index, bounds string,
	iterator func(key, value string) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	if index == "" {
		// cannot search on keys tree. just return nil.
		return nil
	}
	// wrap a rtree specific iterator around the user-defined iterator.
	var clock lazyClock
	iter := func(item rtred.Item) bool {
		dbi := item.(*dbItem)
		if clock.expired(dbi) {
			return true
		}
		return iterator(dbi.key, dbi.val)
	}
	idx := tx.db.idxs[index]
	if idx == nil {
		// index was not found. return error
		return ErrNotFound
	}
	if idx.rtr == nil {
		// not an r-tree index. just return nil
		return nil
	}
	// execute the search
	var min, max []float64
	if idx.rect != nil {
		min, max = idx.rect(bounds)
	}
	if tx.wc != nil {
		tx.wc.itercount++
		defer func() {
			tx.wc.itercount--
		}()
	}
	idx.rtr.Search(&rect{min, max}, iter)
	return nil
}

// Len returns the number of items in the database. The count includes expired
// items that the background task has not yet deleted.
func (tx *Tx) Len() (int, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	return tx.db.keys.Len(), nil
}

// IndexOptions sets extra options for an index.
type IndexOptions struct {
	// CaseInsensitiveKeyMatching makes the index match keys without regard to
	// case. The index converts the pattern to lower case when you create the
	// index. It converts a key to lower case only if the key holds an ASCII upper
	// case letter. A key with only non-ASCII upper case letters stays unchanged,
	// so a pattern with such letters can miss it.
	CaseInsensitiveKeyMatching bool
}

// CreateIndex builds a new index and fills it with items. The index orders the
// items in a B-tree. Read them with the Ascend and Descend functions.
// CreateIndex returns ErrIndexExists if an index with the same name exists, or
// if the name is empty.
//
// The pattern selects the keys that the index holds. In the pattern, '*'
// matches any number of characters, and '?' matches any one character.
//
// The less function returns true if string a sorts before string b. It sets
// the order of the index. The strings can hold text or binary data. The less
// function must handle the format of its data. IndexString, IndexBinary,
// IndexInt, IndexUint and IndexFloat are ready-made less functions. IndexJSON
// and IndexJSONCaseSensitive return less functions. Call one with a path, for
// example IndexJSON("age").
//
// If you pass more than one less function, the index orders by the first. It
// uses each next function only when the earlier ones find the values equal.
// Items with equal values sort by key.
//
// CreateIndex returns ErrTxNotWritable in a read-only transaction. It returns
// ErrTxIterating inside an iteration callback.
func (tx *Tx) CreateIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return tx.createIndex(name, pattern, less, nil, nil)
}

// CreateIndexOptions is the same as CreateIndex, with the extra argument opts.
// A nil opts uses the default options.
func (tx *Tx) CreateIndexOptions(name, pattern string,
	opts *IndexOptions,
	less ...func(a, b string) bool) error {
	return tx.createIndex(name, pattern, less, nil, opts)
}

// CreateSpatialIndex builds a new spatial index and fills it with items. The
// index stores the items in an R-tree. Search them with Intersects and Nearby.
// CreateSpatialIndex returns ErrIndexExists if an index with the same name
// exists, or if the name is empty.
//
// The rect function converts the value of an item to a rectangle. It returns
// the rectangle as two slices, min and max. Each slice has 1 to 20 values, one
// for each dimension. Both slices must have the same length. A length of 1 is
// a one-dimensional rectangle. A length of 4 is a four-dimensional rectangle.
//
// At each dimension, the value of min must be less than or equal to the value
// of max. For example, min[0] must be less than or equal to max[0].
//
// The index does not hold an item whose slices have different lengths, or a
// length of 0 or more than 20. IndexRect is a ready-made rect function.
//
// CreateSpatialIndex returns ErrTxNotWritable in a read-only transaction. It
// returns ErrTxIterating inside an iteration callback.
func (tx *Tx) CreateSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return tx.createIndex(name, pattern, nil, rect, nil)
}

// CreateSpatialIndexOptions is the same as CreateSpatialIndex, with the extra
// argument opts. A nil opts uses the default options.
func (tx *Tx) CreateSpatialIndexOptions(name, pattern string,
	opts *IndexOptions,
	rect func(item string) (min, max []float64)) error {
	return tx.createIndex(name, pattern, nil, rect, opts)
}

// createIndex is called by CreateIndex() and CreateSpatialIndex()
func (tx *Tx) createIndex(name string, pattern string,
	lessers []func(a, b string) bool,
	rect func(item string) (min, max []float64),
	opts *IndexOptions,
) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}
	if name == "" {
		// cannot create an index without a name.
		// an empty name index is designated for the main "keys" tree.
		return ErrIndexExists
	}
	// check if an index with that name already exists.
	if _, ok := tx.db.idxs[name]; ok {
		// index with name already exists. error.
		return ErrIndexExists
	}
	// genreate a less function
	var less func(a, b string) bool
	switch len(lessers) {
	default:
		// multiple less functions specified.
		// create a compound less function.
		less = func(a, b string) bool {
			for i := 0; i < len(lessers)-1; i++ {
				if lessers[i](a, b) {
					return true
				}
				if lessers[i](b, a) {
					return false
				}
			}
			return lessers[len(lessers)-1](a, b)
		}
	case 0:
		// no less function
	case 1:
		less = lessers[0]
	}
	var sopts IndexOptions
	if opts != nil {
		sopts = *opts
	}
	if sopts.CaseInsensitiveKeyMatching {
		pattern = strings.ToLower(pattern)
	}
	// intialize new index
	idx := &index{
		name:    name,
		pattern: pattern,
		less:    less,
		rect:    rect,
		db:      tx.db,
		opts:    sopts,
	}
	idx.rebuild()
	// save the index
	tx.db.idxs[name] = idx
	if tx.wc.rbkeys == nil {
		// store the index in the rollback map.
		if tx.wc.rollbackIndexes == nil {
			tx.wc.rollbackIndexes = make(map[string]*index)
		}
		if _, ok := tx.wc.rollbackIndexes[name]; !ok {
			// we use nil to indicate that the index should be removed upon
			// rollback.
			tx.wc.rollbackIndexes[name] = nil
		}
	}
	return nil
}

// DropIndex removes an index. It returns ErrNotFound if the index does not
// exist. It returns ErrInvalidOperation if name is empty. It returns
// ErrTxNotWritable in a read-only transaction, and ErrTxIterating inside an
// iteration callback.
func (tx *Tx) DropIndex(name string) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}
	if name == "" {
		// cannot drop the default "keys" index
		return ErrInvalidOperation
	}
	idx, ok := tx.db.idxs[name]
	if !ok {
		return ErrNotFound
	}
	// delete from the map.
	// this is all that is needed to delete an index.
	delete(tx.db.idxs, name)
	if tx.wc.rbkeys == nil {
		// store the index in the rollback map.
		if tx.wc.rollbackIndexes == nil {
			tx.wc.rollbackIndexes = make(map[string]*index)
		}
		if _, ok := tx.wc.rollbackIndexes[name]; !ok {
			// we use a non-nil copy of the index without the data to indicate
			// that the index should be rebuilt upon rollback.
			tx.wc.rollbackIndexes[name] = idx.clearCopy()
		}
	}
	return nil
}

// Indexes returns the names of the indexes in sorted order.
func (tx *Tx) Indexes() ([]string, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	names := make([]string, 0, len(tx.db.idxs))
	for name := range tx.db.idxs {
		names = append(names, name)
	}
	slices.Sort(names)
	return names, nil
}

// Rect returns the string form of a rectangle with the given min and max.
// IndexRect is the reverse function: it converts the string to a rectangle.
func Rect(min, max []float64) string {
	r := grect.Rect{Min: min, Max: max}
	return r.String()
}

// Point returns the string form of a point with the given coordinates. The
// point is a rectangle with equal min and max.
func Point(coords ...float64) string {
	return Rect(coords, coords)
}

// IndexRect converts a string to a rectangle. It is a ready-made rect function
// for CreateSpatialIndex. Rect is the reverse function: it converts a
// rectangle to a string.
func IndexRect(a string) (min, max []float64) {
	r := grect.Get(a)
	return r.Min, r.Max
}

// IndexString is a less function. It returns true if a is less than b. The
// comparison ignores the case of ASCII letters. Use IndexBinary for a
// case-sensitive comparison.
func IndexString(a, b string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] >= 'A' && a[i] <= 'Z' {
			if b[i] >= 'A' && b[i] <= 'Z' {
				// both are uppercase, do nothing
				if a[i] < b[i] {
					return true
				} else if a[i] > b[i] {
					return false
				}
			} else {
				// a is uppercase, convert a to lowercase
				if a[i]+32 < b[i] {
					return true
				} else if a[i]+32 > b[i] {
					return false
				}
			}
		} else if b[i] >= 'A' && b[i] <= 'Z' {
			// b is uppercase, convert b to lowercase
			if a[i] < b[i]+32 {
				return true
			} else if a[i] > b[i]+32 {
				return false
			}
		} else {
			// neither are uppercase
			if a[i] < b[i] {
				return true
			} else if a[i] > b[i] {
				return false
			}
		}
	}
	return len(a) < len(b)
}

// IndexBinary is a less function. It returns true if a is less than b, when it
// compares the raw bytes of the strings.
func IndexBinary(a, b string) bool {
	return a < b
}

// IndexInt is a less function. It returns true if a is less than b, when it
// compares the strings as base-10 int64 values. A string with invalid syntax
// counts as 0.
func IndexInt(a, b string) bool {
	ia, _ := strconv.ParseInt(a, 10, 64)
	ib, _ := strconv.ParseInt(b, 10, 64)
	return ia < ib
}

// IndexUint is a less function. It returns true if a is less than b, when it
// compares the strings as base-10 uint64 values. A string with invalid syntax
// counts as 0.
func IndexUint(a, b string) bool {
	ia, _ := strconv.ParseUint(a, 10, 64)
	ib, _ := strconv.ParseUint(b, 10, 64)
	return ia < ib
}

// IndexFloat is a less function. It returns true if a is less than b, when it
// compares the strings as float64 values. A string with invalid syntax counts
// as 0.
func IndexFloat(a, b string) bool {
	ia, _ := strconv.ParseFloat(a, 64)
	ib, _ := strconv.ParseFloat(b, 64)
	return ia < ib
}

// IndexJSON returns a less function that compares the JSON field at path in
// two values. The path uses the GJSON syntax. A string field compares without
// regard to the case of ASCII letters. Call IndexJSON with a path. Pass the
// result to CreateIndex. Do not pass IndexJSON itself.
func IndexJSON(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), false)
	}
}

// IndexJSONCaseSensitive returns a less function that compares the JSON field
// at path in two values. The path uses the GJSON syntax. A string field
// compares with regard to case. Call IndexJSONCaseSensitive with a path. Pass
// the result to CreateIndex. Do not pass IndexJSONCaseSensitive itself.
func IndexJSONCaseSensitive(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), true)
	}
}

// Desc returns a less function with the opposite order of less.
func Desc(less func(a, b string) bool) func(a, b string) bool {
	return func(a, b string) bool { return less(b, a) }
}

//// Wrappers around btree Ascend/Descend

// dbTree is the b-tree behind the keys, expiration and index trees: a v2
// Table on *dbItem. The Table keeps its comparator private, so the tree keeps
// a copy for bLT and bGT.
type dbTree struct {
	*btree.Table[*dbItem]
	less func(a, b *dbItem) bool
}

func bLT(tr *dbTree, a, b *dbItem) bool { return tr.less(a, b) }
func bGT(tr *dbTree, a, b *dbItem) bool { return tr.less(b, a) }

// walk calls iter for each item of seq until iter returns false.
func walk(seq iter.Seq[*dbItem], iter func(item *dbItem) bool) {
	for item := range seq {
		if !iter(item) {
			return
		}
	}
}

// ascendFrom and descendFrom start at pivot. A nil pivot starts at the end of
// the tree, as in btree v1; v2 would pass it to the comparator.
func ascendFrom(tr *dbTree, pivot *dbItem) iter.Seq[*dbItem] {
	if pivot == nil {
		return tr.All()
	}
	return tr.Ascend(pivot)
}

func descendFrom(tr *dbTree, pivot *dbItem) iter.Seq[*dbItem] {
	if pivot == nil {
		return tr.Backward()
	}
	return tr.Descend(pivot)
}

// Ascend

func btreeAscend(tr *dbTree, iter func(item *dbItem) bool) {
	walk(tr.All(), iter)
}

func btreeAscendLessThan(tr *dbTree, pivot *dbItem,
	iter func(item *dbItem) bool,
) {
	walk(tr.All(), func(item *dbItem) bool {
		return bLT(tr, item, pivot) && iter(item)
	})
}

func btreeAscendGreaterOrEqual(tr *dbTree, pivot *dbItem,
	iter func(item *dbItem) bool,
) {
	walk(ascendFrom(tr, pivot), iter)
}

func btreeAscendRange(tr *dbTree, greaterOrEqual, lessThan *dbItem,
	iter func(item *dbItem) bool,
) {
	walk(ascendFrom(tr, greaterOrEqual), func(item *dbItem) bool {
		return bLT(tr, item, lessThan) && iter(item)
	})
}

// Descend

func btreeDescend(tr *dbTree, iter func(item *dbItem) bool) {
	walk(tr.Backward(), iter)
}

func btreeDescendGreaterThan(tr *dbTree, pivot *dbItem,
	iter func(item *dbItem) bool,
) {
	walk(tr.Backward(), func(item *dbItem) bool {
		return bGT(tr, item, pivot) && iter(item)
	})
}

func btreeDescendRange(tr *dbTree, lessOrEqual, greaterThan *dbItem,
	iter func(item *dbItem) bool,
) {
	walk(descendFrom(tr, lessOrEqual), func(item *dbItem) bool {
		return bGT(tr, item, greaterThan) && iter(item)
	})
}

func btreeDescendLessOrEqual(tr *dbTree, pivot *dbItem,
	iter func(item *dbItem) bool,
) {
	walk(descendFrom(tr, pivot), iter)
}

// btreeNew returns an empty tree. v2 has no locks to turn off, and a fixed
// fanout of 64 (at most 63 items per node), so the Degree 128 of btree v1.8.2
// is not available. Retained memory is still below v1.8.2 with Degree 128,
// because the typed tree stores 8-byte item slots: 0.954 (plain), 0.892 (TTL)
// and 0.650 (secondary index) of it, in bench/round12/step7/mem-gate-u2.txt.
func btreeNew(less func(a, b *dbItem) bool) *dbTree {
	return &dbTree{btree.NewTableOptions(btree.TableOptions[*dbItem]{Less: less}), less}
}
