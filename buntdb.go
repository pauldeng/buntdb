// Package buntdb implements a low-level in-memory key/value store in pure Go.
// It persists to disk, is ACID compliant, and uses locking for multiple
// readers and a single writer. Bunt is ideal for projects that need a
// dependable database, and favor speed over data size.
package buntdb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/btree"
	"github.com/tidwall/gjson"
	"github.com/tidwall/grect"
	"github.com/tidwall/match"
	"github.com/tidwall/rtred"
)

var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx closed")

	// ErrNotFound is returned when an item or index is not in the database.
	ErrNotFound = errors.New("not found")

	// ErrInvalid is returned when the database file is an invalid format.
	ErrInvalid = errors.New("invalid database")

	// ErrDatabaseClosed is returned when the database is closed.
	ErrDatabaseClosed = errors.New("database closed")

	// ErrIndexExists is returned when an index already exists in the database.
	ErrIndexExists = errors.New("index exists")

	// ErrInvalidOperation is returned when an operation cannot be completed.
	ErrInvalidOperation = errors.New("invalid operation")

	// ErrInvalidSyncPolicy is returned for an invalid SyncPolicy value.
	ErrInvalidSyncPolicy = errors.New("invalid sync policy")

	// ErrShrinkInProcess is returned when a shrink operation is in-process.
	ErrShrinkInProcess = errors.New("shrink is in-process")

	// ErrPersistenceActive is returned when post-loading data from an database
	// not opened with Open(":memory:").
	ErrPersistenceActive = errors.New("persistence active")

	// ErrTxIterating is returned when Set or Delete are called while iterating.
	ErrTxIterating = errors.New("tx is iterating")
)

// DB represents a collection of key-value pairs that persist on disk.
// Transactions are used for all forms of data access to the DB.
type DB struct {
	mu        sync.RWMutex      // the gatekeeper for all fields
	file      *os.File          // the underlying file
	buf       []byte            // a buffer to write to
	keys      *btree.BTree      // a tree of all item ordered by key
	exps      *btree.BTree      // a tree of items ordered by expiration
	idxs      map[string]*index // the index trees.
	insIdxs   []*index          // a reuse buffer for gathering indexes
	flushes   int               // a count of the number of disk flushes
	closed    bool              // set when the database has been closed
	config    Config            // the database configuration
	persist   bool              // do we write to disk
	shrinking bool              // when an aof shrink is in-process.
	lastaofsz int64             // the size of the last shrink aof size
}

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy int

const (
	// Never is used to disable syncing data to disk.
	// The faster and less safe method.
	Never SyncPolicy = 0
	// EverySecond is used to sync data to disk every second.
	// It's pretty fast and you can lose 1 second of data if there
	// is a disaster.
	// This is the recommended setting.
	EverySecond = 1
	// Always is used to sync data after every write to disk.
	// Slow. Very safe.
	// A failed sync is returned from Commit. See Commit for what that does
	// and does not promise.
	Always = 2
)

// Config represents database configuration options. These
// options are used to change various behaviors of the database.
type Config struct {
	// SyncPolicy adjusts how often the data is synced to disk.
	// This value can be Never, EverySecond, or Always.
	// The default is EverySecond.
	SyncPolicy SyncPolicy

	// AutoShrinkPercentage is used by the background process to trigger
	// a shrink of the aof file when the size of the file is larger than the
	// percentage of the result of the previous shrunk file.
	// For example, if this value is 100, and the last shrink process
	// resulted in a 100mb file, then the new aof file must be 200mb before
	// a shrink is triggered.
	AutoShrinkPercentage int

	// AutoShrinkMinSize defines the minimum size of the aof file before
	// an automatic shrink can occur.
	AutoShrinkMinSize int

	// AutoShrinkDisabled turns off automatic background shrinking
	AutoShrinkDisabled bool

	// OnExpired is used to custom handle the deletion option when a key
	// has been expired.
	OnExpired func(keys []string)

	// OnExpiredSync will be called inside the same transaction that is
	// performing the deletion of expired items. If OnExpired is present then
	// this callback will not be called. If this callback is present, then the
	// deletion of the timeed-out item is the explicit responsibility of this
	// callback.
	OnExpiredSync func(key, value string, tx *Tx) error
}

// exctx is a simple b-tree context for ordering by expiration.
type exctx struct {
	db *DB
}

// Open opens a database at the provided path.
// If the file does not exist then it will be created automatically.
func Open(path string) (*DB, error) {
	db := &DB{}
	// initialize trees and indexes
	db.keys = btreeNew(lessCtx(nil))
	db.exps = btreeNew(lessCtx(&exctx{db}))
	db.idxs = make(map[string]*index)
	// initialize default configuration
	db.config = Config{
		SyncPolicy:           EverySecond,
		AutoShrinkPercentage: 100,
		AutoShrinkMinSize:    32 * 1024 * 1024,
	}
	// turn off persistence for pure in-memory
	db.persist = path != ":memory:"
	if db.persist {
		var err error
		// hardcoding 0666 as the default mode.
		db.file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		// load the database from disk
		if err := db.load(); err != nil {
			// close on error, ignore close error
			_ = db.file.Close()
			return nil, err
		}
	}
	// start the background manager.
	go db.backgroundManager()
	return db, nil
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
// Close syncs the file before closing it.
// Close releases every resource even when the sync or close fails.
// Close returns any error from syncing or closing the file.
// Close returns ErrDatabaseClosed if the database is already closed.
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

// Save writes a snapshot of the database to a writer. This operation blocks all
// writes, but not reads. This can be used for snapshots and backups for pure
// in-memory databases using the ":memory:". Database that persist to disk
// can be snapshotted by simply copying the database file.
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
	btreeAscend(db.keys, func(item interface{}) bool {
		dbi := item.(*dbItem)
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

// Load loads commands from reader. This operation blocks all reads and writes.
// Note that this can only work for fully in-memory databases opened with
// Open(":memory:").
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
	btr     *btree.BTree                           // contains the items
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
		nidx.btr = btreeNew(lessCtx(nidx))
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
		idx.btr = btreeNew(lessCtx(idx))
	}
	if idx.rect != nil {
		idx.rtr = rtred.New(idx)
	}
	// iterate through all keys and fill the index
	btreeAscend(idx.db.keys, func(item interface{}) bool {
		dbi := item.(*dbItem)
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

// CreateIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// An error will occur if an index with the same name already exists.
//
// When a pattern is provided, the index will be populated with
// keys that match the specified pattern. This is a very simple pattern
// match where '*' matches on any number characters and '?' matches on
// any one character.
// The less function compares if string 'a' is less than string 'b'.
// It allows for indexes to create custom ordering. It's possible
// that the strings may be textual or binary. It's up to the provided
// less function to handle the content format and comparison.
// There are some default less function that can be used such as
// IndexString, IndexBinary, etc.
func (db *DB) CreateIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return db.Update(func(tx *Tx) error {
		return tx.CreateIndex(name, pattern, less...)
	})
}

// ReplaceIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// If a previous index with the same name exists, that index will be deleted.
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

// CreateSpatialIndex builds a new index and populates it with items.
// The items are organized in an r-tree and can be retrieved using the
// Intersects method.
// An error will occur if an index with the same name already exists.
//
// The rect function converts a string to a rectangle. The rectangle is
// represented by two arrays, min and max. Both arrays may have a length
// between 1 and 20, and both arrays must match in length. A length of 1 is a
// one dimensional rectangle, and a length of 4 is a four dimension rectangle.
// There is support for up to 20 dimensions.
// The values of min must be less than the values of max at the same dimension.
// Thus min[0] must be less-than-or-equal-to max[0].
// The IndexRect is a default function that can be used for the rect
// parameter.
func (db *DB) CreateSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return db.Update(func(tx *Tx) error {
		return tx.CreateSpatialIndex(name, pattern, rect)
	})
}

// ReplaceSpatialIndex builds a new index and populates it with items.
// The items are organized in an r-tree and can be retrieved using the
// Intersects method.
// If a previous index with the same name exists, that index will be deleted.
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

// DropIndex removes an index.
func (db *DB) DropIndex(name string) error {
	return db.Update(func(tx *Tx) error {
		return tx.DropIndex(name)
	})
}

// Indexes returns a list of index names.
func (db *DB) Indexes() ([]string, error) {
	var names []string
	var err = db.View(func(tx *Tx) error {
		var err error
		names, err = tx.Indexes()
		return err
	})
	return names, err
}

// ReadConfig returns the database configuration.
func (db *DB) ReadConfig(config *Config) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	*config = db.config
	return nil
}

// SetConfig updates the database configuration.
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
	prev := db.keys.Set(item)
	if prev != nil {
		// A previous item was removed from the keys tree. Let's
		// fully delete this item from all indexes.
		pdbi = prev.(*dbItem)
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
	prev := db.keys.Delete(item)
	if prev != nil {
		pdbi = prev.(*dbItem)
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
func (db *DB) backgroundManager() {
	flushes := 0
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		var shrink bool
		// Open a standard view. This will take a full lock of the
		// database thus allowing for access to anything we need.
		var onExpired func([]string)
		var expired []*dbItem
		var onExpiredSync func(key, value string, tx *Tx) error
		err := db.Update(func(tx *Tx) error {
			onExpired = db.config.OnExpired
			if onExpired == nil {
				onExpiredSync = db.config.OnExpiredSync
			}
			if db.persist && !db.config.AutoShrinkDisabled {
				pos, err := db.file.Seek(0, 1)
				if err != nil {
					return err
				}
				shrink = pos > int64(db.config.AutoShrinkMinSize) &&
					shrinkDue(pos, db.lastaofsz, db.config.AutoShrinkPercentage)
			}
			// produce a list of expired items that need removing
			btreeAscendLessThan(db.exps, &dbItem{
				opts: &dbItemOpts{ex: true, exat: time.Now()},
			}, func(item interface{}) bool {
				expired = append(expired, item.(*dbItem))
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
		if err == ErrDatabaseClosed {
			break
		}

		// send expired event, if needed
		if onExpired != nil && len(expired) > 0 {
			keys := make([]string, 0, 32)
			for _, itm := range expired {
				keys = append(keys, itm.key)
			}
			onExpired(keys)
		}

		// execute a disk sync, if needed
		func() {
			db.mu.Lock()
			defer db.mu.Unlock()
			if !db.closed && db.persist && db.config.SyncPolicy == EverySecond &&
				flushes != db.flushes {
				// Advance only on success, so a failed sync is retried.
				if db.file.Sync() == nil {
					flushes = db.flushes
				}
			}
		}()
		if shrink {
			if err = db.Shrink(); err != nil {
				if err == ErrDatabaseClosed {
					break
				}
			}
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

// Shrink will make the database file smaller by removing redundant
// log entries. This operation does not block the database.
//
// Shrink writes the new file to a temporary file in the same directory. It
// syncs that file before it replaces the old one, so a crash leaves either
// the old file or the new file, complete.
//
// The new file keeps the permission bits of the old file. On Unix it also
// keeps the owner and group, and on Linux the POSIX ACL. If Shrink cannot copy
// the owner or the ACL, the new file gets at most mode 0600 and no ACL. Shrink
// does not report this narrowing. If Shrink cannot remove an inherited ACL, it
// returns the error and leaves the database open on the old file.
// On Windows the new file inherits its DACL from the directory.
// Shrink does not copy an explicit DACL from the old file.
//
// If a failed Shrink leaves the database file unusable, Shrink closes the
// database and returns the error. Later calls return ErrDatabaseClosed.
// Shrink never panics for this reason. If only the final directory sync
// fails, the database stays open and uses the new file.
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
	fname := db.file.Name()
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
				func(item interface{}) bool {
					dbi := item.(*dbItem)
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
		return syncDir(filepath.Dir(fname))
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
		line, err := r.ReadBytes('\n')
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
			line, err := r.ReadBytes('\n')
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
			// copy string
			parts = append(parts, string(data[:size]))
			cmdByteSize += int64(size + 2)
		}
		// finished reading the command

		if len(parts) == 0 {
			totalSize += pending + cmdByteSize
			pending = 0
			continue
		}
		// Each command is matched by exact length, then by byte. The length
		// check restores the guard commit f240a9c had and b559e28 dropped to
		// admit the seven-character "flushdb": without it, indexing parts[0]
		// panicked on a shorter name. It also keeps matching ASCII-only, so a
		// multi-byte look-alike such as "ſet" (U+017F) cannot pass as SET.
		if len(parts[0]) == 3 &&
			(parts[0][0] == 's' || parts[0][0] == 'S') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 't' || parts[0][2] == 'T') {
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
		} else if len(parts[0]) == 3 &&
			(parts[0][0] == 'd' || parts[0][0] == 'D') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 'l' || parts[0][2] == 'L') {
			// DEL
			if len(parts) != 2 {
				return totalSize, ErrInvalid
			}
			db.deleteFromDatabase(&dbItem{key: parts[1]})
		} else if len(parts[0]) == 7 && strings.EqualFold(parts[0], "flushdb") {
			db.keys = btreeNew(lessCtx(nil))
			db.exps = btreeNew(lessCtx(&exctx{db}))
			db.idxs = make(map[string]*index)
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
	db.keys.Walk(func(items []interface{}) {
		for _, v := range items {
			estaofsz += int64(v.(*dbItem).estAOFSetSize())
		}
	})
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

// View executes a function within a managed read-only transaction.
// When a non-nil error is returned from the function that error will be return
// to the caller of View().
//
// If fn panics or calls runtime.Goexit, the transaction is rolled back.
// Any panic continues to propagate to the caller.
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB) View(fn func(tx *Tx) error) error {
	return db.managed(false, fn)
}

// Update executes a function within a managed read/write transaction.
// If fn returns an error, the transaction is rolled back and that error is
// returned to the caller. If fn returns nil, the transaction is committed and
// any error from Commit is returned.
//
// Under SyncPolicy Always, a sync error leaves the changes applied with
// durability unconfirmed; it does not mean the transaction was rolled back.
// See Commit before retrying a non-idempotent operation after an error.
//
// The transaction is committed only when fn returns nil. If fn panics or calls
// runtime.Goexit, the transaction is rolled back, discarding its writes.
// Any panic continues to propagate to the caller.
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB) Update(fn func(tx *Tx) error) error {
	return db.managed(true, fn)
}

// get return an item or nil if not found.
func (db *DB) get(key string) *dbItem {
	item := db.keys.Get(&dbItem{key: key})
	if item != nil {
		return item.(*dbItem)
	}
	return nil
}

// Tx represents a transaction on the database. This transaction can either be
// read-only or read/write. Read-only transactions can be used for retrieving
// values for keys and iterating through keys and values. Read/write
// transactions can set and delete keys.
//
// All transactions must be committed or rolled-back when done.
type Tx struct {
	db       *DB             // the underlying database.
	writable bool            // when false mutable operations fail.
	funcd    bool            // when true Commit and Rollback panic.
	wc       *txWriteContext // context for writable transactions.
}

type txWriteContext struct {
	// rollback when deleteAll is called
	rbkeys *btree.BTree      // a tree of all item ordered by key
	rbexps *btree.BTree      // a tree of items ordered by expiration
	rbidxs map[string]*index // the index trees.

	rollbackItems   map[string]*dbItem // details for rolling back tx.
	commitItems     map[string]*dbItem // details for committing tx.
	itercount       int                // stack of iterators
	rollbackIndexes map[string]*index  // details for dropped indexes.
}

// DeleteAll deletes all items from the database.
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
	tx.db.keys = btreeNew(lessCtx(nil))
	tx.db.exps = btreeNew(lessCtx(&exctx{tx.db}))
	tx.db.idxs = make(map[string]*index, len(idxs))

	// finally re-create the current indexes, not the ones saved for rollback
	for name, idx := range idxs {
		tx.db.idxs[name] = idx.clearCopy()
	}

	// always clear out the commits
	tx.wc.commitItems = make(map[string]*dbItem)

	return nil
}

// Begin opens a new transaction.
// Multiple read-only transactions can be opened at the same time but there can
// only be one read/write transaction at a time. Attempting to open a read/write
// transactions while another one is in progress will result in blocking until
// the current read/write transaction is completed.
//
// All transactions must be closed by calling Commit() or Rollback() when done.
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

// Commit writes all changes to disk.
// An error is returned when a write error occurs, or when a Commit() is called
// from a read-only transaction.
//
// Under SyncPolicy Always a failed fsync is also reported. In that case the
// append itself succeeded and the in-memory changes are kept: the error means
// durability is unconfirmed, NOT that the transaction was rolled back. Callers
// must not treat it as a rollback and retry a non-idempotent operation, or
// they will apply it twice. A write error is never replaced by a later sync
// result. Subsequent operations remain allowed, but a sync failure does not
// establish that storage is healthy.
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

// Rollback closes the transaction and reverts all mutable operations that
// were performed on the transaction such as Set() and Delete().
//
// Read-only transactions can only be rolled back, not committed.
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

// Less determines if a b-tree item is less than another. This is required
// for ordering, inserting, and deleting items from a b-tree. It's important
// to note that the ctx parameter is used to help with determine which
// formula to use on an item. Each b-tree should use a different ctx when
// sharing the same item.
func (dbi *dbItem) Less(dbi2 *dbItem, ctx interface{}) bool {
	switch ctx := ctx.(type) {
	case *exctx:
		// The expires b-tree formula
		if dbi2.expiresAt().After(dbi.expiresAt()) {
			return true
		}
		if dbi.expiresAt().After(dbi2.expiresAt()) {
			return false
		}
	case *index:
		if ctx.less != nil {
			// Using an index
			if ctx.less(dbi.val, dbi2.val) {
				return true
			}
			if ctx.less(dbi2.val, dbi.val) {
				return false
			}
		}
	}
	// Always fall back to the key comparison. This creates absolute uniqueness.
	if dbi.keyless {
		return false
	} else if dbi2.keyless {
		return true
	}
	return dbi.key < dbi2.key
}

func lessCtx(ctx interface{}) func(a, b interface{}) bool {
	return func(a, b interface{}) bool {
		return a.(*dbItem).Less(b.(*dbItem), ctx)
	}
}

// Rect converts a string to a rectangle.
// An invalid rectangle will cause a panic.
func (dbi *dbItem) Rect(ctx interface{}) (min, max []float64) {
	switch ctx := ctx.(type) {
	case *index:
		return ctx.rect(dbi.val)
	}
	return nil, nil
}

// SetOptions represents options that may be included with the Set() command.
type SetOptions struct {
	// Expires indicates that the Set() key-value will expire
	Expires bool
	// TTL is how much time the key-value will exist in the database
	// before being evicted. The Expires field must also be set to true.
	// TTL stands for Time-To-Live.
	TTL time.Duration
}

// GetLess returns the less function for an index. This is handy for
// doing ad-hoc compares inside a transaction.
// Returns ErrNotFound if the index is not found or there is no less
// function bound to the index
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

// GetRect returns the rect function for an index. This is handy for
// doing ad-hoc searches inside a transaction.
// Returns ErrNotFound if the index is not found or there is no rect
// function bound to the index
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

// Set inserts or replaces an item in the database based on the key.
// The opt params may be used for additional functionality such as forcing
// the item to be evicted at a specified time. When the return value
// for err is nil the operation succeeded. When the return value of
// replaced is true, then the operaton replaced an existing item whose
// value will be returned through the previousValue variable.
// The results of this operation will not be available to other
// transactions until the current transaction has successfully committed.
//
// Only a writable transaction can be used with this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
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

// Get returns a value for a key. If the item does not exist or if the item
// has expired then ErrNotFound is returned. If ignoreExpired is true, then
// the found value will be returned even if it is expired.
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

// Delete removes an item from the database based on the item's key. If the item
// does not exist or if the item has expired then ErrNotFound is returned.
//
// Only a writable transaction can be used for this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
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

// TTL returns the remaining time-to-live for an item.
// A negative duration will be returned for items that do not have an
// expiration.
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
	iter := func(item interface{}) bool {
		dbi := item.(*dbItem)
		if dbi.expired() {
			return true
		}
		return iterator(dbi.key, dbi.val)
	}
	var tr *btree.BTree
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

// Match returns true if the specified key matches the pattern. This is a very
// simple pattern matcher where '*' matches on any number characters and '?'
// matches on any one character.
func Match(key, pattern string) bool {
	return match.Match(key, pattern)
}

// AscendKeys allows for iterating through keys based on the specified pattern.
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

// DescendKeys allows for iterating through keys based on the specified pattern.
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

// Ascend calls the iterator for every item in the database within the range
// [first, last], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) Ascend(index string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, false, false, index, "", "", iterator)
}

// AscendGreaterOrEqual calls the iterator for every item in the database within
// the range [pivot, last], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendGreaterOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, true, false, index, pivot, "", iterator)
}

// AscendLessThan calls the iterator for every item in the database within the
// range [first, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendLessThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, false, true, index, pivot, "", iterator)
}

// AscendRange calls the iterator for every item in the database within
// the range [greaterOrEqual, lessThan), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendRange(index, greaterOrEqual, lessThan string,
	iterator func(key, value string) bool) error {
	return tx.scan(
		false, true, true, index, greaterOrEqual, lessThan, iterator,
	)
}

// Descend calls the iterator for every item in the database within the range
// [last, first], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) Descend(index string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, false, false, index, "", "", iterator)
}

// DescendGreaterThan calls the iterator for every item in the database within
// the range [last, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendGreaterThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, true, false, index, pivot, "", iterator)
}

// DescendLessOrEqual calls the iterator for every item in the database within
// the range [pivot, first], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendLessOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, false, true, index, pivot, "", iterator)
}

// DescendRange calls the iterator for every item in the database within
// the range [lessOrEqual, greaterThan), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendRange(index, lessOrEqual, greaterThan string,
	iterator func(key, value string) bool) error {
	return tx.scan(
		true, true, true, index, lessOrEqual, greaterThan, iterator,
	)
}

// AscendEqual calls the iterator for every item in the database that equals
// pivot, until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
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

// DescendEqual calls the iterator for every item in the database that equals
// pivot, until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
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

func (r *rect) Rect(ctx interface{}) (min, max []float64) {
	return r.min, r.max
}

// Nearby searches for rectangle items that are nearby a target rect.
// All items belonging to the specified index will be returned in order of
// nearest to farthest.
// The specified index must have been created by AddIndex() and the target
// is represented by the rect string. This string will be processed by the
// same bounds function that was passed to the CreateSpatialIndex() function.
// An invalid index will return an error.
// The dist param is the distance of the bounding boxes. In the case of
// simple 2D points, it's the distance of the two 2D points squared.
//
// Nearby skips expired items, as Get does. It checks expiry against one clock
// reading per call. The search stops when the iterator returns false.
//
// In a writable transaction, Set and Delete return ErrTxIterating inside the
// iterator. In a read-only transaction, they return ErrTxNotWritable.
//
// Nearby sorts by distance only among items with the same number of
// dimensions. An index that holds rects of different dimensions can return
// them out of order.
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

// Intersects searches for rectangle items that intersect a target rect.
// The specified index must have been created by AddIndex() and the target
// is represented by the rect string. This string will be processed by the
// same bounds function that was passed to the CreateSpatialIndex() function.
// An invalid index will return an error.
//
// Intersects skips expired items, as Get does. It checks expiry against one
// clock reading per call. The search stops when the iterator returns false.
//
// In a writable transaction, Set and Delete return ErrTxIterating inside the
// iterator. In a read-only transaction, they return ErrTxNotWritable.
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

// Len returns the number of items in the database
func (tx *Tx) Len() (int, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	return tx.db.keys.Len(), nil
}

// IndexOptions provides an index with additional features or
// alternate functionality.
type IndexOptions struct {
	// CaseInsensitiveKeyMatching allow for case-insensitive
	// matching on keys when setting key/values.
	CaseInsensitiveKeyMatching bool
}

// CreateIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// An error will occur if an index with the same name already exists.
//
// When a pattern is provided, the index will be populated with
// keys that match the specified pattern. This is a very simple pattern
// match where '*' matches on any number characters and '?' matches on
// any one character.
// The less function compares if string 'a' is less than string 'b'.
// It allows for indexes to create custom ordering. It's possible
// that the strings may be textual or binary. It's up to the provided
// less function to handle the content format and comparison.
// There are some default less function that can be used such as
// IndexString, IndexBinary, etc.
func (tx *Tx) CreateIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return tx.createIndex(name, pattern, less, nil, nil)
}

// CreateIndexOptions is the same as CreateIndex except that it allows
// for additional options.
func (tx *Tx) CreateIndexOptions(name, pattern string,
	opts *IndexOptions,
	less ...func(a, b string) bool) error {
	return tx.createIndex(name, pattern, less, nil, opts)
}

// CreateSpatialIndex builds a new index and populates it with items.
// The items are organized in an r-tree and can be retrieved using the
// Intersects method.
// An error will occur if an index with the same name already exists.
//
// The rect function converts a string to a rectangle. The rectangle is
// represented by two arrays, min and max. Both arrays may have a length
// between 1 and 20, and both arrays must match in length. A length of 1 is a
// one dimensional rectangle, and a length of 4 is a four dimension rectangle.
// There is support for up to 20 dimensions.
// The values of min must be less than the values of max at the same dimension.
// Thus min[0] must be less-than-or-equal-to max[0].
// The IndexRect is a default function that can be used for the rect
// parameter.
func (tx *Tx) CreateSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return tx.createIndex(name, pattern, nil, rect, nil)
}

// CreateSpatialIndexOptions is the same as CreateSpatialIndex except that
// it allows for additional options.
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

// DropIndex removes an index.
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

// Indexes returns a list of index names.
func (tx *Tx) Indexes() ([]string, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	names := make([]string, 0, len(tx.db.idxs))
	for name := range tx.db.idxs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// Rect is helper function that returns a string representation
// of a rect. IndexRect() is the reverse function and can be used
// to generate a rect from a string.
func Rect(min, max []float64) string {
	r := grect.Rect{Min: min, Max: max}
	return r.String()
}

// Point is a helper function that converts a series of float64s
// to a rectangle for a spatial index.
func Point(coords ...float64) string {
	return Rect(coords, coords)
}

// IndexRect is a helper function that converts string to a rect.
// Rect() is the reverse function and can be used to generate a string
// from a rect.
func IndexRect(a string) (min, max []float64) {
	r := grect.Get(a)
	return r.Min, r.Max
}

// IndexString is a helper function that return true if 'a' is less than 'b'.
// This is a case-insensitive comparison. Use the IndexBinary() for comparing
// case-sensitive strings.
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

// IndexBinary is a helper function that returns true if 'a' is less than 'b'.
// This compares the raw binary of the string.
func IndexBinary(a, b string) bool {
	return a < b
}

// IndexInt is a helper function that returns true if 'a' is less than 'b'.
func IndexInt(a, b string) bool {
	ia, _ := strconv.ParseInt(a, 10, 64)
	ib, _ := strconv.ParseInt(b, 10, 64)
	return ia < ib
}

// IndexUint is a helper function that returns true if 'a' is less than 'b'.
// This compares uint64s that are added to the database using the
// Uint() conversion function.
func IndexUint(a, b string) bool {
	ia, _ := strconv.ParseUint(a, 10, 64)
	ib, _ := strconv.ParseUint(b, 10, 64)
	return ia < ib
}

// IndexFloat is a helper function that returns true if 'a' is less than 'b'.
// This compares float64s that are added to the database using the
// Float() conversion function.
func IndexFloat(a, b string) bool {
	ia, _ := strconv.ParseFloat(a, 64)
	ib, _ := strconv.ParseFloat(b, 64)
	return ia < ib
}

// IndexJSON provides for the ability to create an index on any JSON field.
// When the field is a string, the comparison will be case-insensitive.
// It returns a helper function used by CreateIndex.
func IndexJSON(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), false)
	}
}

// IndexJSONCaseSensitive provides for the ability to create an index on
// any JSON field.
// When the field is a string, the comparison will be case-sensitive.
// It returns a helper function used by CreateIndex.
func IndexJSONCaseSensitive(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), true)
	}
}

// Desc is a helper function that changes the order of an index.
func Desc(less func(a, b string) bool) func(a, b string) bool {
	return func(a, b string) bool { return less(b, a) }
}

//// Wrappers around btree Ascend/Descend

func bLT(tr *btree.BTree, a, b interface{}) bool { return tr.Less(a, b) }
func bGT(tr *btree.BTree, a, b interface{}) bool { return tr.Less(b, a) }

// func bLTE(tr *btree.BTree, a, b interface{}) bool { return !tr.Less(b, a) }
// func bGTE(tr *btree.BTree, a, b interface{}) bool { return !tr.Less(a, b) }

// Ascend

func btreeAscend(tr *btree.BTree, iter func(item interface{}) bool) {
	tr.Ascend(nil, iter)
}

func btreeAscendLessThan(tr *btree.BTree, pivot interface{},
	iter func(item interface{}) bool,
) {
	tr.Ascend(nil, func(item interface{}) bool {
		return bLT(tr, item, pivot) && iter(item)
	})
}

func btreeAscendGreaterOrEqual(tr *btree.BTree, pivot interface{},
	iter func(item interface{}) bool,
) {
	tr.Ascend(pivot, iter)
}

func btreeAscendRange(tr *btree.BTree, greaterOrEqual, lessThan interface{},
	iter func(item interface{}) bool,
) {
	tr.Ascend(greaterOrEqual, func(item interface{}) bool {
		return bLT(tr, item, lessThan) && iter(item)
	})
}

// Descend

func btreeDescend(tr *btree.BTree, iter func(item interface{}) bool) {
	tr.Descend(nil, iter)
}

func btreeDescendGreaterThan(tr *btree.BTree, pivot interface{},
	iter func(item interface{}) bool,
) {
	tr.Descend(nil, func(item interface{}) bool {
		return bGT(tr, item, pivot) && iter(item)
	})
}

func btreeDescendRange(tr *btree.BTree, lessOrEqual, greaterThan interface{},
	iter func(item interface{}) bool,
) {
	tr.Descend(lessOrEqual, func(item interface{}) bool {
		return bGT(tr, item, greaterThan) && iter(item)
	})
}

func btreeDescendLessOrEqual(tr *btree.BTree, pivot interface{},
	iter func(item interface{}) bool,
) {
	tr.Descend(pivot, iter)
}

func btreeNew(less func(a, b interface{}) bool) *btree.BTree {
	// NoLocks because we're managing our own locks. Degree 128 keeps the
	// btree v1.4.2 node layout (at most 255 items per node). The v1.8.2 default
	// of 32 failed the memory gate (+5.9% plain, +16.8% index); the evidence is
	// in bench/round12/step6a-u1/.
	return btree.NewOptions(less, btree.Options{NoLocks: true, Degree: 128})
}
