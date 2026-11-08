<p align="center">
<img
    src="logo.png"
    width="307" height="150" border="0" alt="BuntDB">
<br>
<a href="https://godoc.org/github.com/tidwall/buntdb"><img src="https://img.shields.io/badge/go-documentation-blue.svg?style=flat-square" alt="Godoc"></a>
<a href="https://github.com/tidwall/buntdb/blob/master/LICENSE"><img src="https://img.shields.io/github/license/tidwall/buntdb.svg?style=flat-square" alt="LICENSE"></a>
</p>

BuntDB is a low-level, in-memory key/value store in pure Go.
It persists to disk and is ACID compliant.
It uses locking for multiple readers and one writer.
It supports custom indexes and geospatial data.
It is ideal for projects that need a dependable database and favor speed over data size.

This repository is a fork of [tidwall/buntdb](https://github.com/tidwall/buntdb).
The fork changes some behavior.
See [Changes from upstream](#changes-from-upstream).

Terms
=====

- An **item** is one key and its value.
- A **TTL** (time to live) is the time that an item stays in the database before it expires.
- A **sync** asks the operating system to write file data to the storage device.
  On Unix, the system call is `fsync`.
- The **database file** is the append-only file (AOF) that holds the log of the database.

Features
========

- In-memory database for [fast reads and writes](#performance)
- Embeddable with a [simple API](https://godoc.org/github.com/tidwall/buntdb)
- [Spatial indexing](#spatial-indexes) for up to 20 dimensions, which is useful for geospatial data
- Index fields inside [JSON](#json-indexes) documents
- [Collate i18n indexes](#collate-i18n-indexes) with the optional [collate package](https://github.com/tidwall/collate)
- Create [custom indexes](#custom-indexes) for any data type
- Support for [multi-value indexes](#multi-value-index), which are similar to a SQL multi-column index
- [Built-in types](#built-in-types) that are easy to use: string, uint, int and float
- Flexible [iteration](#iterating) of data: ascending, descending and ranges
- [Durable append-only file](#append-only-file) format for persistence
- Optional [expiration](#data-expiration) of old items with a TTL
- ACID semantics with locking [transactions](#transactions) that support rollbacks

Changes from upstream
=====================

This fork differs from upstream tidwall/buntdb at commit `0dbc8c1`.
Each change in the table can break code or data that works with upstream.
The IDs match the commit messages of this repository.
The text uses the past tense only for upstream or earlier behavior and for measurements.

| ID | Change | Details |
|---|---|---|
| C3 | The database file stores an expiration time before 1970 as a signed number. `Open` returns `ErrInvalid` for an invalid expiration value. | [Expiration times in the database file](#expiration-times-in-the-database-file) |
| C5 | In a read/write transaction, a write inside an `Intersects` or `Nearby` callback returns `ErrTxIterating`. | [Expired items and writes during a search](#expired-items-and-writes-during-a-search) |
| C6 | `Shrink` syncs a temporary file before the rename, keeps the file permissions, and does not panic. | [Shrink behavior](#shrink-behavior) |
| D1 | `Intersects` and `Nearby` skip expired items. | [Expired items and writes during a search](#expired-items-and-writes-during-a-search) |
| D2 | `Close` returns the error of its final sync. | [Sync errors](#sync-errors) |
| U1b | GJSON v1.20.1 changes the order of some JSON index values. | [JSON index changes with GJSON v1.20.1](#json-index-changes-with-gjson-v1201) |
| U2 | btree v2 makes scans over many items slower. | [Performance changes with btree v2](#performance-changes-with-btree-v2) |
| S1 | `DB.Sync` syncs the database file and its directory at a known point. | [Sync at a known point](#sync-at-a-known-point) |
| T1, T2 | The expiration sweep takes the write lock only when an item is due, and the `EverySecond` sync runs without a lock. | [Background task](#background-task) |

[Other changes](#other-changes-from-upstream) lists more differences.

Getting Started
===============

## Installing

To use BuntDB, install Go.
Then run `go get`:

```sh
$ go get -u github.com/tidwall/buntdb
```

This command retrieves the upstream module `github.com/tidwall/buntdb`.
To use this fork, clone it.
Then add a `replace` directive to the `go.mod` file of your project.
The path in the directive is the directory of the clone:

```
replace github.com/tidwall/buntdb => ../buntdb
```

This fork needs Go 1.27.3 or later.

## Opening a database

The primary object in BuntDB is a `DB`.
To open or create your database, use the `buntdb.Open()` function:

```go
package main

import (
	"log"

	"github.com/tidwall/buntdb"
)

func main() {
	// Open the data.db file. BuntDB creates it if it does not exist.
	db, err := buntdb.Open("data.db")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Print(err)
		}
	}()

	// Use db here.
}
```

To open an in-memory database, use `:memory:` as the path:

```go
buntdb.Open(":memory:") // Open an in-memory database. It creates no file.
```

## Transactions
All reads and writes run inside a transaction.
BuntDB runs one read/write transaction at a time.
It runs many read-only transactions at the same time.
Each transaction has a stable view of the database.
After a transaction begins, other transactions cannot change its data.

A transaction runs in a function that receives a `Tx` object.
The `Tx` object holds the state of the transaction.
Inside a transaction, use only the `Tx` object for database operations.
Do not use the `DB` object inside a transaction.
This can block your application.

If the function returns an error, the transaction rolls back.
A rollback reverts all changes that the transaction made.
If the function panics or calls `runtime.Goexit`, the transaction also rolls back.
If a read/write transaction returns nil, BuntDB commits the changes.
For a database that persists to disk, a commit appends the item changes to the database file.

### Read-only Transactions
Use a read-only transaction when you do not change data.
Many read-only transactions can run at the same time.

```go
err := db.View(func(tx *buntdb.Tx) error {
	// Read data with tx here.
	return nil
})
```

### Read/write Transactions
Use a read/write transaction when you change data.
Only one read/write transaction can run at a time.
Return from the function as soon as the work ends.

```go
err := db.Update(func(tx *buntdb.Tx) error {
	// Change data with tx here.
	return nil
})
```

## Setting and getting items

To set a value, open a read/write transaction:

```go
err := db.Update(func(tx *buntdb.Tx) error {
	_, _, err := tx.Set("mykey", "myvalue", nil)
	return err
})
```

To get the value:

```go
err := db.View(func(tx *buntdb.Tx) error {
	val, err := tx.Get("mykey")
	if err != nil {
		return err
	}
	fmt.Printf("value is %s\n", val)
	return nil
})
```

`Get` returns `ErrNotFound` if the item does not exist or has expired.

### Iterating
The database orders all items by key.
To iterate over the keys:

```go
err := db.View(func(tx *buntdb.Tx) error {
	err := tx.Ascend("", func(key, value string) bool {
		fmt.Printf("key: %s, value: %s\n", key, value)
		return true // continue iteration
	})
	return err
})
```

The `Tx` object also has `AscendGreaterOrEqual`, `AscendLessThan`, `AscendRange`, `AscendEqual`, `Descend`, `DescendLessOrEqual`, `DescendGreaterThan`, `DescendRange` and `DescendEqual`.
See the [documentation](https://godoc.org/github.com/tidwall/buntdb) for these functions.
Iteration skips expired items.


## Custom Indexes
At first, the database stores all items in one [B-tree](https://en.wikipedia.org/wiki/B-tree).
Each item has one key and one value.
The B-tree orders the items by key.
This order gives fast lookup of a value by key.
It also gives fast [iteration](#iterating) over the keys.

A custom index orders the items by value.
Use it to [iterate](#iterating) over the values in that order.
A custom index also uses a B-tree.
It is more flexible, because you choose the order.

For example, this call creates an index that orders names:

```go
db.CreateIndex("names", "*", buntdb.IndexString)
```

This call creates an index named `names`.
The index stores and sorts all values.
The second argument is a pattern that filters keys.
The wildcard `*` accepts all keys.
`IndexString` is a built-in function that orders values without regard to the case of ASCII letters.

Now add some names:

```go
db.Update(func(tx *buntdb.Tx) error {
	tx.Set("user:0:name", "tom", nil)
	tx.Set("user:1:name", "Randi", nil)
	tx.Set("user:2:name", "jane", nil)
	tx.Set("user:4:name", "Janet", nil)
	tx.Set("user:5:name", "Paula", nil)
	tx.Set("user:6:name", "peter", nil)
	tx.Set("user:7:name", "Terri", nil)
	return nil
})
```

Finally, iterate over the index:

```go
db.View(func(tx *buntdb.Tx) error {
	tx.Ascend("names", func(key, val string) bool {
		fmt.Printf("%s %s\n", key, val)
		return true
	})
	return nil
})
```

The output is:

```
user:2:name jane
user:4:name Janet
user:5:name Paula
user:6:name peter
user:1:name Randi
user:7:name Terri
user:0:name tom
```

The pattern argument filters keys.
To change the pattern, replace the index, as in this example:

```go
if err := db.ReplaceIndex("names", "user:*", buntdb.IndexString); err != nil {
	log.Fatal(err)
}
```

Now the `names` index holds only items whose keys start with `user:`.


### Built-in types
Along with `IndexString`, the package has `IndexBinary`, `IndexInt`, `IndexUint` and `IndexFloat`.
`IndexInt`, `IndexUint` and `IndexFloat` compare the values as numbers.
`IndexBinary` compares the raw bytes of the values.
You can use these functions or write your own.

To create an index that orders an age key by number, use:

```go
db.CreateIndex("ages", "user:*:age", buntdb.IndexInt)
```

Then add values:

```go
db.Update(func(tx *buntdb.Tx) error {
	tx.Set("user:0:age", "35", nil)
	tx.Set("user:1:age", "49", nil)
	tx.Set("user:2:age", "13", nil)
	tx.Set("user:4:age", "63", nil)
	tx.Set("user:5:age", "8", nil)
	tx.Set("user:6:age", "3", nil)
	tx.Set("user:7:age", "16", nil)
	return nil
})
```

```go
db.View(func(tx *buntdb.Tx) error {
	tx.Ascend("ages", func(key, val string) bool {
		fmt.Printf("%s %s\n", key, val)
		return true
	})
	return nil
})
```

The output is:

```
user:6:age 3
user:5:age 8
user:2:age 13
user:7:age 16
user:0:age 35
user:1:age 49
user:4:age 63
```

## Spatial Indexes
BuntDB supports spatial indexes.
A spatial index stores rectangles in an [R-tree](https://en.wikipedia.org/wiki/R-tree).
An R-tree is a balanced tree, like a [B-tree](https://en.wikipedia.org/wiki/B-tree).
An R-tree can also handle data in many dimensions.
This makes it useful for geospatial applications.

To create a spatial index, use the `CreateSpatialIndex` function:

```go
db.CreateSpatialIndex("fleet", "fleet:*:pos", buntdb.IndexRect)
```

`IndexRect` is a built-in function that converts rectangle strings to a format that the R-tree can use.
You can use it as it is.
You can also write your own function for another format, such as [Well-known text](https://en.wikipedia.org/wiki/Well-known_text) or [GeoJSON](http://geojson.org/).

To add some lon,lat points to the `fleet` index:

```go
db.Update(func(tx *buntdb.Tx) error {
	tx.Set("fleet:0:pos", "[-115.567 33.532]", nil)
	tx.Set("fleet:1:pos", "[-116.671 35.735]", nil)
	tx.Set("fleet:2:pos", "[-113.902 31.234]", nil)
	return nil
})
```

Then run the `Intersects` function on the index:

```go
db.View(func(tx *buntdb.Tx) error {
	tx.Intersects("fleet", "[-117 30],[-112 36]", func(key, val string) bool {
		// Use key and val here.
		return true
	})
	return nil
})
```

This call finds all three positions.

### k-Nearest Neighbors

Use the `Nearby` function to get all positions in order, from nearest to farthest.
The `dist` argument is the squared distance between the bounding boxes.

```go
db.View(func(tx *buntdb.Tx) error {
	tx.Nearby("fleet", "[-113 33]", func(key, val string, dist float64) bool {
		// Use key, val and dist here.
		return true
	})
	return nil
})
```

### Expired items and writes during a search

`Intersects` and `Nearby` skip expired items, as `Get` does.
The search continues past an expired item.
Each call checks expiration against one clock reading.
Earlier versions returned expired items.

The search stops when the callback returns `false`.

Do not write to the database inside a callback.
In a read/write transaction, a write such as `Set` or `Delete` returns `ErrTxIterating`.
In a read-only transaction, a write returns `ErrTxNotWritable`.
Earlier versions allowed writes inside `Intersects` and `Nearby` callbacks.

### Nearby limits

`Nearby` sorts by distance only among rectangles with the same number of dimensions.
An index that holds both 1-D and 2-D rectangles can return items out of order.

In an index with mixed dimensions, a callback that panics or calls `runtime.Goexit` can leak goroutines.
`testing.T.Fatal` calls `runtime.Goexit`.
Keep fatal test assertions outside such a callback.

In an index with mixed dimensions, background goroutines can remain after the callback returns `false`.
Do not rely on `Nearby` to finish before a following write.

These limits come from the `github.com/tidwall/rtred` package.
An index whose rectangles all have the same number of dimensions does not have them.

### Spatial bracket syntax

The bracket syntax `[-117 30],[-112 36]` is unique to BuntDB.
`IndexRect` processes it for the built-in rectangles.
You can use another syntax.
The function that you pass to `CreateSpatialIndex` processes the argument.
In these examples, that function is `IndexRect`.

- **2-D rectangle:** `[10 15],[20 25]`
*Min XY: "10x15", Max XY: "20x25"*

- **3-D rectangle:** `[10 15 12],[20 25 18]`
*Min XYZ: "10x15x12", Max XYZ: "20x25x18"*

- **2-D point:** `[10 15]`
*XY: "10x15"*

- **LonLat point:** `[-112.2693 33.5123]`
*Lon: -112.2693, Lat: 33.5123*

- **LonLat bounding box:** `[-112.26 33.51],[-112.18 33.67]`
*Min Lon/Lat: "-112.26 33.51", Max Lon/Lat: "-112.18 33.67"*

**Notice:** In a LonLat value, the first number is longitude, the X axis.
The second number is latitude, the Y axis.

Use `-inf` and `+inf` to represent infinity.
For example, assume these points.
Each point is `[X Y M]`, where `XY` is a position and `M` is a timestamp:
```
[3 9 1]
[3 8 2]
[4 8 3]
[4 7 4]
[5 7 5]
[5 6 6]
```

To find all points with `M` between 2 and 4, call `Intersects`:

```go
tx.Intersects("points", "[-inf -inf 2],[+inf +inf 4]", func(key, val string) bool {
	println(val)
	return true
})
```

This call returns:

```
[3 8 2]
[4 8 3]
[4 7 4]
```

## JSON Indexes
You can create an index on a field inside JSON documents.
BuntDB uses [GJSON](https://github.com/tidwall/gjson) for this.

For example:

```go
package main

import (
	"fmt"

	"github.com/tidwall/buntdb"
)

func main() {
	db, _ := buntdb.Open(":memory:")
	db.CreateIndex("last_name", "*", buntdb.IndexJSON("name.last"))
	db.CreateIndex("age", "*", buntdb.IndexJSON("age"))
	db.Update(func(tx *buntdb.Tx) error {
		tx.Set("1", `{"name":{"first":"Tom","last":"Johnson"},"age":38}`, nil)
		tx.Set("2", `{"name":{"first":"Janet","last":"Prichard"},"age":47}`, nil)
		tx.Set("3", `{"name":{"first":"Carol","last":"Anderson"},"age":52}`, nil)
		tx.Set("4", `{"name":{"first":"Alan","last":"Cooper"},"age":28}`, nil)
		return nil
	})
	db.View(func(tx *buntdb.Tx) error {
		fmt.Println("Order by last name")
		tx.Ascend("last_name", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		fmt.Println("Order by age")
		tx.Ascend("age", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		fmt.Println("Order by age range 30-50")
		tx.AscendRange("age", `{"age":30}`, `{"age":50}`, func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		return nil
	})
}
```

Results:

```
Order by last name
3: {"name":{"first":"Carol","last":"Anderson"},"age":52}
4: {"name":{"first":"Alan","last":"Cooper"},"age":28}
1: {"name":{"first":"Tom","last":"Johnson"},"age":38}
2: {"name":{"first":"Janet","last":"Prichard"},"age":47}
Order by age
4: {"name":{"first":"Alan","last":"Cooper"},"age":28}
1: {"name":{"first":"Tom","last":"Johnson"},"age":38}
2: {"name":{"first":"Janet","last":"Prichard"},"age":47}
3: {"name":{"first":"Carol","last":"Anderson"},"age":52}
Order by age range 30-50
1: {"name":{"first":"Tom","last":"Johnson"},"age":38}
2: {"name":{"first":"Janet","last":"Prichard"},"age":47}
```

### JSON index changes with GJSON v1.20.1

This fork uses GJSON v1.20.1. Upstream commit `0dbc8c1` uses GJSON v1.14.3.
The upgrade changes the order of `IndexJSON` and `IndexJSONCaseSensitive` indexes for the inputs below.
`CreateIndex` rebuilds an index from the stored data, so existing data needs no migration.
In benchmarks, setting an item in a JSON index took about 4% longer with GJSON v1.20.1.

- **Surrogate escapes.** A high surrogate escape without a following low surrogate escape decodes to U+FFFD alone. The next escape stays separate.
  For example, `"\uD800\u0041"` now decodes to `"\uFFFDA"`. It was `"\uFFFD"`, so it sorted before `"\uFFFD0"` and now sorts after it.
  Two high surrogates, `"\uD800\uD800"`, now decode to two U+FFFD characters instead of one. A low surrogate followed by an escape also decodes alone.
- **`~null`, `~false` and `~*` queries.** In an index path such as `a.#(v==~null).id`, `~null` now matches null and missing values. It matched nothing before.
  `~false` now matches only false, null, missing values, `0` and strings that parse as false, such as `"false"`. It no longer matches other strings, arrays or objects.
  `~*` now matches every existing value. An unknown name, such as `~foo`, now matches nothing. An item that matched before can sort differently.
- **Escaped keys before a dot pipeline.** A path such as `fav\.movie.[0]` now follows the dot pipeline `.[0]` after the escaped key. GJSON v1.14.3 ignored it.
  Documents `{"fav.movie":[1]}` and `{"fav.movie":[2]}` compared equal before and now sort by the array element.
- **Array indexes above uint64.** A path such as `a.18446744073709551617` no longer wraps around to index 1. The index saturates, so the path finds nothing.
- **Modifiers.** `gjson.DisableModifiers` no longer has an effect, so `@` paths always run. `gjson.AddModifier` ignores a stock modifier name such as `reverse`.
  A path that uses either setting sorts differently. To disable modifiers, build with `-ldflags="-X 'github.com/tidwall/gjson.disableModifiers=true'"`.
- **New `@dig` modifier.** The path `@dig` no longer reads a field named `@dig`. It runs the modifier, which returns `[]` for both documents, so `{"@dig":1}` and `{"@dig":2}` compare equal.
  Write `\@dig` to read the field.
- **Multipath keys.** A multipath such as `"{\x08:a,\n:b}"` (Go string syntax) escapes its key names differently. Two classes of key bytes change.
  GJSON now writes backspace and form feed as `\b` and `\f`, instead of `\u0008` and `\u000c`.
  GJSON now writes every invalid UTF-8 byte as a literal U+FFFD, instead of the escape `\ufffd`.
  The raw result changes, so the order against other keys can flip. Valid UTF-8 and all other control characters stay the same.
- **Modifier arguments.** A bracketed part of a modifier argument, such as `@this:unused(1|2)`, no longer splits at the `|` inside it. The path now selects the whole document.
- **Modifier numbers.** A string argument such as `{"width":"80.0"}` for `@pretty` now parses as 80. It was 0, so the formatted output and its order change.

## Multi-Value Index
A multi-value index joins many values in one index.
It is similar to a [multi-column index](http://dev.mysql.com/doc/refman/5.7/en/multiple-column-indexes.html) in a traditional SQL database.

This example creates a multi-value index on "name.last" and "age":

```go
db, _ := buntdb.Open(":memory:")
db.CreateIndex("last_name_age", "*", buntdb.IndexJSON("name.last"), buntdb.IndexJSON("age"))
db.Update(func(tx *buntdb.Tx) error {
	tx.Set("1", `{"name":{"first":"Tom","last":"Johnson"},"age":38}`, nil)
	tx.Set("2", `{"name":{"first":"Janet","last":"Prichard"},"age":47}`, nil)
	tx.Set("3", `{"name":{"first":"Carol","last":"Anderson"},"age":52}`, nil)
	tx.Set("4", `{"name":{"first":"Alan","last":"Cooper"},"age":28}`, nil)
	tx.Set("5", `{"name":{"first":"Sam","last":"Anderson"},"age":51}`, nil)
	tx.Set("6", `{"name":{"first":"Melinda","last":"Prichard"},"age":44}`, nil)
	return nil
})
db.View(func(tx *buntdb.Tx) error {
	tx.Ascend("last_name_age", func(key, value string) bool {
		fmt.Printf("%s: %s\n", key, value)
		return true
	})
	return nil
})

// Output:
// 5: {"name":{"first":"Sam","last":"Anderson"},"age":51}
// 3: {"name":{"first":"Carol","last":"Anderson"},"age":52}
// 4: {"name":{"first":"Alan","last":"Cooper"},"age":28}
// 1: {"name":{"first":"Tom","last":"Johnson"},"age":38}
// 6: {"name":{"first":"Melinda","last":"Prichard"},"age":44}
// 2: {"name":{"first":"Janet","last":"Prichard"},"age":47}
```

## Descending Ordered Index
Wrap the less function of an index with `buntdb.Desc` to put the index in descending order.

```go
err := db.ReplaceIndex("last_name_age", "*",
	buntdb.IndexJSON("name.last"),
	buntdb.Desc(buntdb.IndexJSON("age")),
)
if err != nil {
	log.Fatal(err)
}
```

This call replaces the `last_name_age` index of the previous section.
In the new index, the last name is in ascending order, and the age is in descending order.

## Collate i18n Indexes

The external [collate package](https://github.com/tidwall/collate) can create indexes that sort by a specified language.
This is similar to the [SQL COLLATE keyword](https://msdn.microsoft.com/en-us/library/ms174596.aspx) in traditional databases.

To install:

```
go get -u github.com/tidwall/collate
```

For example:

```go
import "github.com/tidwall/collate"

// To sort case-insensitive in French.
db.CreateIndex("name", "*", collate.IndexString("FRENCH_CI"))

// To sort numbers numerically ("2" < "12")
// and use a comma to represent a decimal point.
db.CreateIndex("amount", "*", collate.IndexString("FRENCH_NUM"))
```

Collation also works for JSON indexes:

```go
db.CreateIndex("last_name", "*", collate.IndexJSON("CHINESE_CI", "name.last"))
```

See the [collate project](https://github.com/tidwall/collate) for more information.

## Data Expiration
Use the `SetOptions` object in the `Set` function to make an item expire after a TTL.

```go
db.Update(func(tx *buntdb.Tx) error {
	tx.Set("mykey", "myval", &buntdb.SetOptions{Expires: true, TTL: time.Second})
	return nil
})
```

After one second, `mykey` is expired.
`Get` returns `ErrNotFound` for an expired item, and iterators skip it.
`Get(key, true)` still returns the value of an expired item.
The [background task](#background-task) deletes the item later, unless a callback in `Config` handles it.
`Len` counts an expired item until the task deletes it.

To remove the TTL, set the item again with the same key and value.
Pass nil as the options argument.

The `OnExpired` and `OnExpiredSync` options of [`Config`](#config) change how the background task handles expired items.
With either option, BuntDB does not delete the expired items.
The callback controls them.
It can delete an item, set it again with a new TTL, or set it again without a TTL.
At each run, the task reports the items that are still expired.

## Background task
`Open` starts a background task.
The task runs about once each second.
Each run does these steps:

1. It handles the expired items.
   It deletes them, or it calls the `OnExpired` or `OnExpiredSync` callback.
2. If the policy is `EverySecond`, it syncs the database file when a commit still needs a sync.
3. If the database file grows enough, it shrinks the file.
   See `AutoShrinkPercentage` and `AutoShrinkMinSize` in [Config](#config).

When items are due, the step that handles them runs one read/write transaction for all of them.
Readers and writers wait while it runs.
An automatic `Shrink` takes its own locks, as [Append-only File](#append-only-file) describes.

**Behavior change.**
Upstream took the write lock in every run, also when no item was due.
A run that waited for the write lock behind a long read-only transaction also held back new read-only transactions.
This fork first looks with the read lock.
It checks whether an item is due and whether a shrink is due.
For the expiration step, it takes the write lock only when an item is due.

**Behavior change.**
Upstream held the write lock during the `EverySecond` sync.
This fork syncs without any lock, so readers and writers do not wait for the sync.
If `Shrink` or `Close` closes the file during the sync, the sync can fail.
The task does not report the failure.
It retries in the next run while the database is open.

## Delete while iterating
BuntDB does not support deleting an item inside an iteration callback.
In a read/write transaction, `Set`, `Delete`, `DeleteAll` and the index-changing methods of the same transaction return `ErrTxIterating` inside the callback.
Collect the keys.
Then delete them after the iteration ends.

```go
var delkeys []string
tx.AscendKeys("object:*", func(k, v string) bool {
	if someCondition(k) == true {
		delkeys = append(delkeys, k)
	}
	return true // continue
})
for _, k := range delkeys {
	if _, err = tx.Delete(k); err != nil {
		return err
	}
}
```

## Append-only File

BuntDB writes an AOF (append-only file).
The AOF is a log of all database changes, from operations such as `Set()` and `Delete()`.

The file uses the RESP format.
This simplified view shows the commands:
```
set key:1 value1
set key:2 value2
set key:1 value3
del key:2
...
```

When the database opens, it reads the file and runs each command in order.
This read happens once, when the database opens.
After that, BuntDB only appends to the file.

The file grows over time.
The background task shrinks the file when it gets too large.
`Shrink()` also rewrites the file.
It writes a snapshot of the items, one command for each item.
It then appends the commands that commit while it runs.
A shrink does not hold a lock for its whole duration.
It reads the items in chunks of about 1000 items, and it holds the read lock for each chunk.
At the end, it holds the write lock while it copies the new commands and replaces the file.

### Expiration times in the database file

A `set` command with an expiration has two extra arguments.
The first is `ae`.
The second is the expiration time as a number of seconds since 1970 (Unix time).
For a time after 1970, the bytes are the same as in upstream.
**Behavior change.**
Upstream wrote a time before 1970 as an unsigned number, and then could not read the file.
This fork writes a signed number.
`Open` still reads the unsigned form of an older file.
`Open` returns `ErrInvalid` for any other expiration value that is not valid.
It also returns `ErrInvalid` for an `ae` time later than the internal time that stands for "never expires".
For a number that is not valid, upstream returned the number-parsing error.

Older files can use the argument `ex` instead of `ae`.
Its value is a number of seconds relative to the modification time of the file.
`Open` treats a modification time in the future as the current time.
Its arithmetic saturates, so a very large value cannot wrap to a wrong time.

### Shrink behavior

`Shrink` writes the new file to a temporary file in the database directory.
It syncs the temporary file before the rename.
The new file has complete contents when it replaces the old file.
On Unix, a directory sync then makes the rename durable.
On other systems, that sync does nothing.
The durability limits of `SyncPolicy` still apply to commits that no sync covered.
The temporary file has a unique name and starts at mode `0600`.
Upstream used the fixed name `<path>.tmp`.

**Behavior change.**
`Shrink` no longer panics when the rename, the reopen or the seek fails.
If it cannot keep a usable database file, it closes the database and returns the error.
Later calls return `ErrDatabaseClosed`.
If only the final directory sync fails, the database stays open and uses the new file.

The new file keeps the permission bits of the old file.
On Unix, it also keeps the owner and the group.
On Linux, it also keeps the POSIX ACL.
If `Shrink` cannot copy the owner or the ACL, the new file gets at most mode `0600` and no ACL.
`Shrink` does not report this narrowing.
If it cannot remove an inherited ACL, it returns the error and the database stays open on the old file.
Earlier versions gave the new file mode `0666` minus the umask, and the default ACL of the directory.

**Windows.** Unix mode bits do not restrict a Windows DACL.
The new file inherits its DACL from the directory, like a file that `Open` creates there.
`Shrink` does not copy an explicit DACL from the old file.

### Durability and sync

By default, BuntDB syncs the [database file](#append-only-file) about once each second.
A crash can then lose the commits since the last sync.
The policy guarantees no maximum loss window, because the background task can run late.
For higher durability, set the option `Config.SyncPolicy` to `Always`.

`Config.SyncPolicy` has these values:

- `Never` - the operating system manages syncs. This is the least safe option.
- `EverySecond` - sync each second. This option is fast and safer. It is the default.
- `Always` - sync after every commit that writes to the file. This option is very durable and slower.

`Close` syncs the file with every policy.

#### Sync errors

With `Always`, the transaction returns a failed sync.
The error means that **durability is unconfirmed, not that the transaction rolled back**.
`Commit` appends the data, and BuntDB keeps the changes in memory.
Do not treat the error as a rollback.
A retry of a non-idempotent operation applies the operation twice.
A later sync error never replaces a write error.
Later operations remain allowed.
A sync failure does not show that the storage is healthy.

The background `EverySecond` sync does not report errors.
It retries at each run until it succeeds.
`Close` returns the error from its final sync or from closing the file.
`Close` closes the file and releases the item and index trees first, so a failed `Close` still leaves the database closed.
Earlier versions ignored the sync error.

#### Sync at a known point

`DB.Sync()` syncs the database file now, and then syncs the directory that holds the file.
Use it with the `Never` or `EverySecond` policy when you need durability at a known point.
When `Sync` returns nil, every write that committed before the call is in the file on disk.
On Unix, the directory entry of the file is on disk too.

```go
err := db.Update(func(tx *buntdb.Tx) error {
	_, _, err := tx.Set("key", "value", nil)
	return err
})
if err == nil {
	err = db.Sync() // call Sync after the transaction, never inside it
}
```

Call `Sync` outside a transaction.
Inside `Update`, it deadlocks.
Inside `View`, it can deadlock when a writer waits.
`Sync` holds no database lock during the file sync and the directory sync.
A write that commits during the `Sync` can be missing from the file on disk.

A `Sync` costs one file sync and then one directory sync.
`Sync` runs them also when no commit is unacknowledged.
If the file sync fails, `Sync` skips the directory sync.
If `Close` or `Shrink` closes the file, `Sync` can make more file sync attempts, as described below.
The directory is the real parent of the file.
`Open` resolves it, and `Shrink` updates it when it replaces a symlink with the file.
A later change of the working directory does not move it.
The directory sync exists on Linux and other Unix systems.
On other systems, the directory sync does nothing, and `Sync` syncs the file only.
`Sync` does not sync the ancestors of that directory.
Those directories must already be durable.
`Close` syncs the file, not the directory.
For an in-memory database, `Sync` does nothing and returns nil.

If the file sync succeeds and the directory sync fails, `Sync` returns the directory error.
The writes are then in the file on disk.
The directory entry of the file can be missing from the disk.
The background task does not repeat the directory sync.
Call `Sync` again.

`Sync` returns `ErrDatabaseClosed` if the database is closed before the call.
It also returns `ErrDatabaseClosed` if `Close` finishes before the file sync starts.
If `Shrink` replaces the file before the file sync starts, `Sync` syncs the new file.
If the file sync fails because `Close` or `Shrink` closed the file, `Sync` tries again.
It makes at most three attempts, then returns the last error.
It returns `ErrDatabaseClosed` instead if the database is closed by then.
If `Close` starts while the file sync runs, that sync finishes.
`Sync` then returns the result of that sync, or the directory error.

The database tracks the sync work conservatively, as acknowledged commit counts.
Each file sync attempt reads the commit count just before it starts.
If the sync succeeds, it acknowledges that count.
That attempt does not acknowledge a write that commits after the read.
After a retry, the call can acknowledge a write that committed during the call.
A concurrent `Sync` can acknowledge a write that another call does not.
A successful `Shrink` and an `Always` sync acknowledge nothing.
The background task can then sync once for writes that are already on disk.
With `EverySecond`, the background task syncs the file while a commit is unacknowledged.
If the file sync of `Sync` fails, it acknowledges nothing, so the background task tries again.
If a sync already acknowledged every commit, nothing retries a failed `Sync`.

## Config

These configuration options change how the database behaves:

- **SyncPolicy** sets how often BuntDB syncs the database file to disk. The value is `Never`, `EverySecond` or `Always`. The default is `EverySecond`.
- **AutoShrinkPercentage** sets when the background task shrinks the database file. The task shrinks the file when it grows by more than this percentage of its size after the previous shrink. For example, assume the value is 100 and the previous shrink made a 100 MiB file. The file must then be larger than 200 MiB before a shrink starts. A value of zero or less means that any growth qualifies. The default is 100.
- **AutoShrinkMinSize** is the size, in bytes, that the file must exceed before an automatic shrink can occur. The default is 32 MiB (32 * 1024 * 1024 bytes).
- **AutoShrinkDisabled** turns off automatic background shrinking. The default is false.
- **OnExpired** is a callback that receives the keys of expired items. The background task calls it outside any transaction. BuntDB does not delete the items. The callback controls them: it can delete an item, set it again with a new TTL, or set it again without a TTL. At each run, the task reports the items that are still expired.
- **OnExpiredSync** is a callback that the background task calls for each expired item, inside the transaction that handles the expired items. BuntDB does not call it when `OnExpired` is set. BuntDB does not delete the item. The callback controls the item through its `tx` argument. It can delete the item, set it again with a new TTL, or set it again without a TTL. If the callback returns an error, BuntDB rolls back the transaction.

To update the configuration, call `ReadConfig`.
Then call `SetConfig`.
For example:

```go

var config buntdb.Config
if err := db.ReadConfig(&config); err != nil {
	log.Fatal(err)
}
if err := db.SetConfig(config); err != nil {
	log.Fatal(err)
}
```

## Performance

How fast is BuntDB?

Here are some example [benchmarks](https://github.com/tidwall/raft-buntdb#raftstore-performance-comparison) of BuntDB in a Raft store implementation.

To run the standard Go benchmark tool, use this command in the project root directory:

```
go test --bench=.
```

### Performance changes with btree v2

This fork stores items in typed tables of btree v2 (`github.com/tidwall/btree/v2` v2.0.0).
The numbers below compare it with the previous build of this fork.
That build used btree v1.8.2 with a node degree of 128.
btree v2 has a fixed fanout of 64 (at most 63 items per node).
It has no degree setting.

- **Scans cost more.** In benchmarks, `Ascend` and `Descend` over 10 to 10,000 items took about 1.3 to 2.0 times as long.
- **`Get` and `Set` with a TTL are faster.** `Get` took about 0.93 times as long. `Set` with a TTL took about 0.76 times as long in memory and 0.86 times as long with persistence.
- **`Set` on a JSON index costs more.** It took about 9% longer.
- **Loading small records is faster.** Loading a database file of small records took about 0.56 times as long.
- **Loading very large values is slower.** Loading a database file of 1 MiB values took about 1.15 to 1.17 times as long.
- **Memory use is lower.** The retained memory was 0.95 times (plain items), 0.89 times (items with a TTL) and 0.65 times (items in a secondary index) the previous value.

The author of this fork accepted these costs.
The target workload reads and sets single items and runs no scans.
The text of this section reports measurements, so it uses the past tense.

### Other performance changes

- **`Intersects` over items with a TTL costs more (D1).** The search took about 12.9% longer, because it now checks expiration. Searches without a TTL and `Nearby` stayed within 5%.
- **A large commit frees its buffer (P2).** A commit with a serialized size above 64 MiB frees its commit buffer after the commit. After a 72 MiB transaction, the live heap was 88.2 MiB smaller.
  Alternating 72 MiB commits and small commits took about 1.9 times as long, because BuntDB regrows the buffer in each cycle.

### BuntDB-Benchmark

A [custom utility](https://github.com/tidwall/buntdb-benchmark) benchmarks BuntDB.

The upstream project measured these results on a MacBook Pro 15" 2.8 GHz Intel Core i7.
They do not include the changes of this fork.

```
$ buntdb-benchmark -q
GET: 4609604.74 operations per second
SET: 248500.33 operations per second
ASCEND_100: 2268998.79 operations per second
ASCEND_200: 1178388.14 operations per second
ASCEND_400: 679134.20 operations per second
ASCEND_800: 348445.55 operations per second
DESCEND_100: 2313821.69 operations per second
DESCEND_200: 1292738.38 operations per second
DESCEND_400: 675258.76 operations per second
DESCEND_800: 337481.67 operations per second
SPATIAL_SET: 134824.60 operations per second
SPATIAL_INTERSECTS_100: 939491.47 operations per second
SPATIAL_INTERSECTS_200: 561590.40 operations per second
SPATIAL_INTERSECTS_400: 306951.15 operations per second
SPATIAL_INTERSECTS_800: 159673.91 operations per second
```

To install this utility:

```
go install github.com/tidwall/buntdb-benchmark@latest
```

## Other changes from upstream

These changes also differ from upstream:

- This fork needs Go 1.27.3 or later.
- `Save` and `Load` return `ErrDatabaseClosed` for a closed database. Upstream panicked.
- With `SyncPolicy` `Always`, `Commit` returns a failed sync. Upstream ignored it. See [Sync errors](#sync-errors).
- If a function that `Update` or `View` runs panics or calls `runtime.Goexit`, the transaction rolls back.
- The loader accepts only the commands `set`, `del` and `flushdb`, in any letter case. It returns `ErrInvalid` for an unknown command name or an invalid RESP header.
  It also returns `ErrInvalid` for bad `set` or `del` arguments, including an invalid expiration value.
  It accepts an empty array (`*0`) and ignores extra arguments of `flushdb`.
  Upstream panicked on a command name shorter than three bytes and on an oversized bulk length.
  Upstream accepted any name that starts with `set` or `del`, such as `settle`, and returned nil for an oversized array count.
  A file that ends in the middle of a command is not an `ErrInvalid` case. `Open` truncates the file after the last complete command, and `Load` returns `io.ErrUnexpectedEOF`.
- `Open` counts NUL padding before a command, and an empty array (`*0`), in the offset where the next commit starts (C1, C2). Upstream did not. After `Open`, the next commit could overwrite the end of a record, and the recovery of a truncated file could cut a valid record.
- The size estimate for an item with a TTL uses `ae` and the exact signed length of the expiration time (C7). Upstream used `ex` and a fixed two-digit number. The estimate sets the size that an automatic shrink compares against after `Open`.
- `CreateSpatialIndexOptions` applies its `IndexOptions` argument. Upstream ignored it.
- In one transaction, a second `DeleteAll` keeps the current index definitions (C9). Upstream restored the index definitions of the first `DeleteAll`, and lost the indexes that you created or dropped in between.
- In the same transaction, `Set` after `DeleteAll` reports `previousValue` and `replaced` for an item that an earlier `Set` wrote (C10). Upstream reported neither.
- A negative `AutoShrinkPercentage` acts as 0 (C13). Upstream used the negative value in its threshold arithmetic.
- The AOF size accounting uses 64-bit integers, so it does not overflow on 32-bit systems (C13).

## Contact
Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

BuntDB source code is available under the MIT [License](/LICENSE).
