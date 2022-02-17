//
// Copyright © 2021 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

extension Database.Statement {
	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(value: Database.Value, toParameter index: Int) throws {
		switch value {
		case .integer(let i):
			guard sqlite3_bind_int64(preparedStatement, Int32(index), i) == SQLITE_OK else {
				throw SQLiteError(fromDatabaseConnection: database.databaseConnection)
			}
		case .real(let r):
			guard sqlite3_bind_double(preparedStatement, Int32(index), r) == SQLITE_OK else {
				throw SQLiteError(fromDatabaseConnection: database.databaseConnection)
			}
		case .text(let t):
			try t.withCString {
				guard sqlite3_bind_text(preparedStatement, Int32(index), $0, -1, SQLiteTransientStorage) == SQLITE_OK else {
					throw SQLiteError(fromDatabaseConnection: database.databaseConnection)
				}
			}
		case .blob(let b):
			try b.withUnsafeBytes {
				guard sqlite3_bind_blob64(preparedStatement, Int32(index), $0.baseAddress, sqlite3_uint64(b.count), SQLiteTransientStorage) == SQLITE_OK else {
					throw SQLiteError(fromDatabaseConnection: database.databaseConnection)
				}
			}
		case .null:
			guard sqlite3_bind_null(preparedStatement, Int32(index)) == SQLITE_OK else {
				throw SQLiteError(fromDatabaseConnection: database.databaseConnection)
			}
		}
	}

	/// Binds `value` to the SQL parameter `name`.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter name: The name of the SQL parameter to bind.
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `value` couldn't be bound.
	public func bind(value: Database.Value, toParameter name: String) throws {
		try bind(value: value, toParameter: indexOfParameter(named: name))
	}
}

extension Database.Statement {
	/// Returns the value of the column at `index` for each row in the result set.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - requires: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	public func column(_ index: Int) throws -> [Database.Value] {
		var values = [Database.Value]()
		try results { row in
			values.append(try row.value(ofColumn: index))
		}
		return values
	}

	/// Returns the value of the column with `name` for each row in the result set.
	///
	/// - parameter name: The name of the desired column.
	///
	/// - throws: An error if the column `name` doesn't exist.
	public func column(_ name: String) throws -> [Database.Value] {
		let index = try index(ofColumn: name)
		var values = [Database.Value]()
		try results { row in
			values.append(try row.value(ofColumn: index))
		}
		return values
	}

	/// Returns the values of the columns at `indexes` for each row in the result set.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - requires: `indexes.min() >= 0`
	/// - requires: `indexes.max() < self.columnCount`
	///
	/// - parameter indexes: The indexes of the desired columns.
	///
	/// - throws: An error if any element of `indexes` is out of bounds.
	public func columns<S: Collection>(_ indexes: S) throws -> [[Database.Value]] where S.Element == Int {
		var values = [[Database.Value]](repeating: [], count: indexes.count)
		try results { row in
			for (n, x) in indexes.enumerated() {
				values[n].append(try row.value(ofColumn: x))
			}
		}
		return values
	}
}
