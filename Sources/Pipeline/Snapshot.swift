//
// Copyright © 2021 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// An `sqlite3_snapshot *` object.
///
/// - seealso: [Database Snapshot](https://www.sqlite.org/c3ref/snapshot.html)
public typealias SQLiteSnapshot = UnsafeMutablePointer<sqlite3_snapshot>

/// The state of a WAL mode database at a specific point in history.
public final class Snapshot {
	/// The owning database
	public let database: Database

	/// The underlying `sqlite3_snapshot *` object
	let snapshot: SQLiteSnapshot

	/// A snapshot of the current state of a database schema.
	///
	/// - note: If a read transaction is not already open one is opened automatically.
	///
	/// - parameter database: The owning database.
	/// - parameter schema: The database schema to snapshot.
	///
	/// - throws: An error if the snapshot could not be recorded.
	init(database: Database, schema: String) throws {
		self.database = database
		var snapshot: SQLiteSnapshot? = nil
		guard sqlite3_snapshot_get(database.databaseConnection, schema, &snapshot) == SQLITE_OK else {
			throw SQLiteError(fromDatabaseConnection: database.databaseConnection)
		}
		precondition(snapshot != nil)
		self.snapshot = snapshot!
	}

	deinit {
		sqlite3_snapshot_free(snapshot)
	}
}

extension Snapshot: Comparable {
	public static func == (lhs: Snapshot, rhs: Snapshot) -> Bool {
//		precondition(lhs.database == rhs.database, "Cannot compare snapshots across databases")
		return sqlite3_snapshot_cmp(lhs.snapshot, rhs.snapshot) == 0
	}

	public static func < (lhs: Snapshot, rhs: Snapshot) -> Bool {
//		precondition(lhs.database == rhs.database, "Cannot compare snapshots across databases")
		return sqlite3_snapshot_cmp(lhs.snapshot, rhs.snapshot) < 0
	}
}
