//
// Copyright © 2021 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// An `sqlite3_stmt *` object.
///
/// - seealso: [SQLite Prepared Statement Object](https://sqlite.org/c3ref/stmt.html)
public typealias SQLitePreparedStatement = OpaquePointer

extension Database {
	/// A compiled SQL statement with support for SQL parameter binding.
	public final class Statement {
		/// The owning database
		public let database: Database
		/// The underlying `sqlite3_stmt *` object
		let preparedStatement: SQLitePreparedStatement

		/// Creates a compiled SQL statement.
		///
		/// - parameter database: The owning database.
		/// - parameter preparedStatement: An `sqlite3_stmt *` prepared statement object..
		///
		/// - throws: An error if `sql` could not be compiled
		public init(database: Database, preparedStatement: SQLitePreparedStatement) {
			precondition(sqlite3_db_handle(preparedStatement) == database.databaseConnection)
			self.database = database
			self.preparedStatement = preparedStatement
		}

		deinit {
			_ = sqlite3_finalize(preparedStatement)
		}

		/// Creates a compiled SQL statement.
		///
		/// - parameter database: The owning database.
		/// - parameter sql: The SQL statement to compile.
		///
		/// - throws: An error if `sql` could not be compiled.
		public convenience init(database: Database, sql: String) throws {
			var stmt: SQLitePreparedStatement?
			guard sqlite3_prepare_v2(database.databaseConnection, sql, -1, &stmt, nil) == SQLITE_OK else {
				throw SQLiteError(fromDatabaseConnection: database.databaseConnection)
			}
			precondition(stmt != nil)
			self.init(database: database, preparedStatement: stmt.unsafelyUnwrapped)
		}

		/// `true` if this statement makes no direct changes to the database, `false` otherwise.
		///
		/// - seealso: [Read-only statements in SQLite](https://sqlite.org/c3ref/stmt_readonly.html)
		public var isReadOnly: Bool {
			sqlite3_stmt_readonly(preparedStatement) != 0
		}

		/// The number of columns in the result set.
		public var columnCount: Int {
			Int(sqlite3_column_count(preparedStatement))
		}

		/// The names of the columns.
		///
		/// - note: Column names are not guaranteed to be unique.
		public lazy var columnNames: [String] = {
			let count = sqlite3_column_count(preparedStatement)
			var names: [String] = []
			for i in 0 ..< count {
				if let s = sqlite3_column_name(preparedStatement, i) {
					names.append(String(cString: s))
				}
			}
			return names
		}()

		/// Returns the name of the column at `index`.
		///
		/// - note: Column indexes are 0-based.  The leftmost column in a result row has index 0.
		///
		/// - parameter index: The index of the desired column.
		///
		/// - throws: An error if `index` is out of bounds.
		///
		/// - returns: The name of the column for the specified index.
		public func name(ofColumn index: Int) throws -> String {
			guard let name = sqlite3_column_name(preparedStatement, Int32(index)) else {
				throw Database.Error(message: "Column index \(index) out of bounds")
			}
			return String(cString: name)
		}

		/// The mapping of column names to indexes
		lazy var columnNamesAndIndexes: [String: Int] = {
			let count = sqlite3_column_count(preparedStatement)
			var map = [String: Int](minimumCapacity: Int(count))
			for i in 0 ..< count {
				if let s = sqlite3_column_name(preparedStatement, i) {
					map[String(cString: s)] = Int(i)
				}
			}
			return map
		}()

		/// Returns the index of the column `name`.
		///
		/// - parameter name: The name of the desired column.
		///
		/// - throws: An error if the column doesn't exist.
		///
		/// - returns: The index of the column with the specified name.
		public func index(ofColumn name: String) throws -> Int {
			guard let index = columnNamesAndIndexes[name] else {
				throw Database.Error(message: "Unknown column \"\(name)\"")
			}
			return index
		}
	}
}

extension Database.Statement {
	/// Performs a low-level SQLite statement operation.
	///
	/// - attention: **Use of this function should be avoided whenever possible.**
	///
	/// - parameter block: A closure performing the statement operation.
	/// - parameter preparedStatement: The raw `sqlite3_stmt *` prepared statement object.
	///
	/// - throws: Any error thrown in `block`.
	///
	/// - returns: The value returned by `block`.
	public func withUnsafeSQLitePreparedStatement<T>(block: (_ preparedStatement: SQLitePreparedStatement) throws -> (T)) rethrows -> T {
		try block(preparedStatement)
	}
}

extension Database.Statement {
	/// Executes the statement and discards any result rows.
	///
	/// - throws: An error if the statement could not be executed.
	public func execute() throws {
		var result = sqlite3_step(preparedStatement)
		while result == SQLITE_ROW {
			result = sqlite3_step(preparedStatement)
		}
		guard result == SQLITE_DONE else {
			throw SQLiteError(fromDatabaseConnection: database.databaseConnection)
		}
	}

	/// Executes the statement and applies `block` to each result row.
	///
	/// - parameter block: A closure applied to each result row.
	/// - parameter row: A result row of returned data.
	///
	/// - throws: Any error thrown in `block` or an error if the statement did not successfully run to completion
	public func results(_ block: ((_ row: Database.Row) throws -> ())) throws {
		var result = sqlite3_step(preparedStatement)
		while result == SQLITE_ROW {
			try block(Database.Row(statement: self))
			result = sqlite3_step(preparedStatement)
		}
		guard result == SQLITE_DONE else {
			throw SQLiteError(fromDatabaseConnection: database.databaseConnection)
		}
	}

	/// Returns the next result row or `nil` if none.
	///
	/// - returns: The next result row of returned data.
	///
	/// - throws: An error if the statement encountered an execution error.
	public func nextRow() throws -> Database.Row? {
		switch sqlite3_step(preparedStatement) {
		case SQLITE_ROW:
			return Database.Row(statement: self)
		case SQLITE_DONE:
			return nil
		default:
			throw SQLiteError(fromDatabaseConnection: database.databaseConnection)
		}
	}

	/// Resets the statement to its initial state, ready to be re-executed.
	///
	/// - note: This function does not change the value of  any bound SQL parameters.
	///
	/// - throws: An error if the statement could not be reset.
	public func reset() throws {
		guard sqlite3_reset(preparedStatement) == SQLITE_OK else {
			throw SQLiteError(fromDatabaseConnection: database.databaseConnection)
		}
	}
}

extension Database.Statement {
	/// The original SQL text of the statement.
	public var sql: String {
		guard let str = sqlite3_sql(preparedStatement) else {
			return ""
		}
		return String(cString: str)
	}

#if SQLITE_ENABLE_NORMALIZE
	/// The normalized SQL text of the statement.
	public var normalizedSQL: String {
		guard let str = sqlite3_normalized_sql(preparedStatement) else {
			return ""
		}
		return String(cString: str)
	}
#endif

	/// The SQL text of the statement with bound parameters expanded.
	public var expandedSQL: String {
		guard let str = sqlite3_expanded_sql(preparedStatement) else {
			return ""
		}
		defer {
			sqlite3_free(str)
		}
		return String(cString: str)
	}
}

extension Database.Statement {
	/// Available statement counters.
	///
	/// - seealso: [Status Parameters for prepared statements](https://www.sqlite.org/c3ref/c_stmtstatus_counter.html)
	public enum	Counter {
		/// The number of times that SQLite has stepped forward in a table as part of a full table scan
		case fullscanStep
		/// The number of sort operations that have occurred
		case sort
		/// The number of rows inserted into transient indices that were created automatically in order to help joins run faster
		case autoindex
		/// The number of virtual machine operations executed by the prepared statement
		case vmStep
		/// The number of times that the prepare statement has been automatically regenerated due to schema changes or change to bound parameters that might affect the query plan
		case reprepare
		/// The number of times that the prepared statement has been run
		case run
		/// The approximate number of bytes of heap memory used to store the prepared statement
		case memused
	}

	/// Returns information on a statement counter.
	///
	/// - parameter counter: The desired statement counter
	/// - parameter reset: If `true` the counter is reset to zero
	///
	/// - returns: The current value of the counter
	///
	/// - seealso: [Prepared Statement Status](https://www.sqlite.org/c3ref/stmt_status.html)
	public func count(of counter: Counter, reset: Bool = false) -> Int {
		let op: Int32
		switch counter {
		case .fullscanStep: 	op = SQLITE_STMTSTATUS_FULLSCAN_STEP
		case .sort:				op = SQLITE_STMTSTATUS_SORT
		case .autoindex:		op = SQLITE_STMTSTATUS_AUTOINDEX
		case .vmStep:			op = SQLITE_STMTSTATUS_VM_STEP
		case .reprepare:		op = SQLITE_STMTSTATUS_REPREPARE
		case .run:				op = SQLITE_STMTSTATUS_RUN
		case .memused:			op = SQLITE_STMTSTATUS_MEMUSED
		}

		return Int(sqlite3_stmt_status(preparedStatement, op, reset ? 1 : 0))
	}
}
