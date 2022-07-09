# Pipeline

Pipeline is a powerful and performant Swift interface to [SQLite](https://sqlite.org) featuring:

- Type-safe and type-agnostic database values.
- Thread-safe synchronous and asynchronous database access.
- Full support for [transactions](#perform-a-transaction) and savepoints.
- [Custom SQL functions](#custom-sql-functions), including aggregate and window functions.
- [Custom collating sequences](#custom-collating-sequences).
- Custom commit, rollback, update, and busy handler hooks.
- Custom virtual tables.
- Custom FTS5 tokenizers.
- Optional support for pre-update hooks and [sessions](https://www.sqlite.org/sessionintro.html).
- [Combine](#combine) support.

Pipeline allows fast, easy database access with robust error handling.  It is not a general-purpose object-relational mapper.

## Installation

### Swift Package Manager

Add a package dependency to https://github.com/sbooth/Pipeline in Xcode.

### Manual or Custom Build

1. Clone the [Pipeline](https://github.com/sbooth/Pipeline) repository.
2. `swift build`.

### CSQLite

Pipeline uses the [CSQLite](https://github.com/sbooth/CSQLite) package for the SQLite library. For performance reasons CSQLite is built without pre-update hook support. In order to enable the pre-update hook in Pipeline it is necessary to clone a local copy of CSQLite and edit the build options appropriately in [Package.swift](https://github.com/sbooth/CSQLite/blob/main/Package.swift).

## Quick Start

```swift
// Create an in-memory database
let database = try Database()

// Create a table
try database.execute(sql: "CREATE TABLE t1(a,b);")

// Insert a row
try database.execute(sql: "INSERT INTO t1(a,b) VALUES (?,?);", 
                     parameters: 33, "lulu")

// Retrieve the values
try database.execute(sql: "SELECT a,b FROM t1;") { row in
    let a = try row.value(at: 0, .int)
    let b = try row.value(at: 1, .string)
}
```

### Segue to Thread Safety

Pipeline compiles SQLite with thread safety disabled for improved performance. While this increases performance, it also means a `Database` instance may only be accessed from a single thread or dispatch queue at a time.

Most applications should not create a `Database` directly but instead should use a thread-safe `DatabaseQueue`.

```swift
// Create a queue serializing access to an in-memory database
let databaseQueue = try DatabaseQueue("myapp.database-isolation-queue")
```

This creates a queue which may be used from multiple threads or dispatch queues safely.  The queue serializes access to the database ensuring only a single operation occurs at a time. Database operations may be performed synchronously or asynchronously.

```swift
// Perform a synchronous database access
try databaseQueue.sync { database in
    // Do something with `database`
}

// Perform an asynchronous database access
databaseQueue.async { database in
    do {
        // Do something with `database`
    } catch let error {
        // Handle any errors that occurred
    }
}
```

For databases using [Write-Ahead Logging](https://www.sqlite.org/wal.html) concurrent reading and writing is supported. Multiple read operations may be performed simultaneously using more than one `DatabaseReadQueue` instance.  Write operations must always be confined to a single `DatabaseQueue`.  A typical usage pattern is one global `DatabaseQueue` instance used for writing located in the application's delegate, with `DatabaseReadQueue` instances located in individual view or window controllers.  When used with long-running read transactions each `DatabaseReadQueue` maintains a separate, consistent snapshot of the database that may be updated in response to database changes.

## Design

The core of Pipeline is the types `Database`, `Statement`, and `Row`.

- `Database` is an SQLite database.

- `Statement` is a compiled SQL statement.

- `Row` is a single result row.

The fundamental type for a column value in a result row is `DatabaseValue`.

- `DatabaseValue` objects contain an integer, floating-point, textual, null, or BLOB value.

- `DatabaseValue` objects may be retrieved from result rows.

Type-safe column value access is provided by specializations of the `ColumnValueConverter` struct.

- `ColumnValueConverter<T>` converts a column value in a result row to an object of type `T`.

Type-safe SQL parameter binding is provided by `DatabaseValue` or `SQLParameterBinder` objects.

- `DatabaseValue` extensions provide database value creation from most common Swift types.

- `SQLParameterBinder` objects capture values not easily represented by `DatabaseValue` and bind them to an SQL parameter.

Thread-safe access to a database is provided by `DatabaseQueue`.

- `DatabaseQueue` serializes work items on a database.
- `DatabaseReadQueue` serializes read operations on a database.

## Examples

### Create an In-Memory Database

```swift
let database = try Database()
```

This creates a database for use on a single thread or dispatch queue only. Most applications should not create a `Database` directly but instead should use a thread-safe `DatabaseQueue`.

### Create a Table

```swift
try database.execute(sql: "CREATE TABLE t1(a,b);")
```

The created table *t1* has two columns, *a* and *b*.

### Insert Data

```swift
for i in 0..<5 {
    try database.execute(sql: "INSERT INTO t1(a,b) VALUES (?,?);",
                         parameters: .int(2*i), .int(2*i+1))
}
```
SQL parameters are passed as a sequence or series of values.  Named parameters are also supported.

```swift
try database.execute(sql: "INSERT INTO t1(a,b) VALUES (:a,:b);",
                     parameters: [":a": 100, ":b": 404])
```

### Insert Data Efficiently

Rather than parsing SQL each time a statement is executed, it is more efficient to prepare a statement and reuse it.

```swift
let statement = try database.prepare(sql: "INSERT INTO t1(a,b) VALUES (?,?);")
for i in 0..<5 {
    try statement.bind(.int(2*i), .int(2*i+1))
    try statement.execute()
    try statement.reset()
    try statement.clearBindings()
}
```

### Fetch Data

The closure passed to `execute()` will be called with each result row.

```swift
try database.execute(sql: "SELECT * FROM t1;") { row in
    let x = try row.value(at: 0, .int)
    let y = try row.valueOrNil(at: 1, .int)
}
```

*row* is a `Row` instance.

### Perform a Transaction

```swift
try database.transaction { database in
    // Do something with `database`
    return .commit
}
```

Database transactions may also be performed asynchronously using `DatabaseQueue`.

```swift
databaseQueue.asyncTransaction { database in
    // Do something with `database`
    return .commit
}
```

### Custom SQL Functions

```swift
let rot13Mapping: [Character: Character] = [
    "A": "N", "B": "O", "C": "P", "D": "Q", "E": "R", "F": "S", "G": "T", "H": "U", "I": "V", "J": "W", "K": "X", "L": "Y", "M": "Z",
    "N": "A", "O": "B", "P": "C", "Q": "D", "R": "E", "S": "F", "T": "G", "U": "H", "V": "I", "W": "J", "X": "K", "Y": "L", "Z": "M",
    "a": "n", "b": "o", "c": "p", "d": "q", "e": "r", "f": "s", "g": "t", "h": "u", "i": "v", "j": "w", "k": "x", "l": "y", "m": "z",
    "n": "a", "o": "b", "p": "c", "q": "d", "r": "e", "s": "f", "t": "g", "u": "h", "v": "i", "w": "j", "x": "k", "y": "l", "z": "m"]

try database.addFunction("rot13", arity: 1) { values in
    let value = values.first.unsafelyUnwrapped
    switch value {
        case .text(let t):
            return .text(String(t.map { rot13Mapping[$0] ?? $0 }))
        default:
            return value
    }
}
```

*rot13* can now be used just like any other [SQL function](https://www.sqlite.org/lang_corefunc.html).

```swift
let statement = try database.prepare(sql: "INSERT INTO t1(a) VALUES (rot13(?));")
```

### Custom Collating Sequences

```swift
try database.addCollation("localized_compare", { (lhs, rhs) -> ComparisonResult in
    return lhs.localizedCompare(rhs)
})
```

*localized_compare* is now available as a [collating sequence](https://www.sqlite.org/c3ref/create_collation.html).

```swift
let statement = try database.prepare(sql: "SELECT * FROM t1 ORDER BY a COLLATE localized_compare;")
```

## Combine

Pipeline provides a Combine publisher for SQLite query results, allowing you to write elegant and powerful data processing code.

```swift
struct UUIDHolder {
	let id: UUID
}

extension UUIDHolder: RowMapping
	init(row: Row) throws {
		id = try row.value(at: 0, .uuidWithString)
	}
}

let database = try Database()

let sevenDaysAgo = Date() - 7 * 24 * 60 * 60

let publisher = database.rowPublisher(sql: "select uuid from table_one where date >= ?;") {
	try $0.bind(.timeIntervalSinceReferenceDate(sevenDaysAgo), toParameter: 1)
}

publisher
	.mapRows(type: UUIDHolder.self)
```

## License

Pipeline is released under the [MIT License](https://github.com/sbooth/Pipeline/blob/main/LICENSE.txt).
