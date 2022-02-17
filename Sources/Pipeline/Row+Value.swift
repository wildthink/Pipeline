//
// Copyright © 2021 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

extension Database.Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value.
	public func value(ofColumn index: Int) throws -> Database.Value {
		let type = sqlite3_column_type(statement.preparedStatement, Int32(index))
		guard sqlite3_errcode(statement.database.databaseConnection) == SQLITE_OK else {
			throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
		}
		switch type {
		case SQLITE_INTEGER:
			let i = sqlite3_column_int64(statement.preparedStatement, Int32(index))
			guard sqlite3_errcode(statement.database.databaseConnection) == SQLITE_OK else {
				throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
			}
			return .integer(i)
		case SQLITE_FLOAT:
			let r = sqlite3_column_double(statement.preparedStatement, Int32(index))
			guard sqlite3_errcode(statement.database.databaseConnection) == SQLITE_OK else {
				throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
			}
			return .real(r)
		case SQLITE_TEXT:
			let t = String(cString: sqlite3_column_text(statement.preparedStatement, Int32(index)))
			guard sqlite3_errcode(statement.database.databaseConnection) == SQLITE_OK else {
				throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
			}
			return .text(t)
		case SQLITE_BLOB:
			guard let b = sqlite3_column_blob(statement.preparedStatement, Int32(index)) else {
				guard sqlite3_errcode(statement.database.databaseConnection) == SQLITE_OK else {
					throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
				}
				return .blob(Data())
			}
			let count = Int(sqlite3_column_bytes(statement.preparedStatement, Int32(index)))
			let data = Data(bytes: b.assumingMemoryBound(to: UInt8.self), count: count)
			return .blob(data)
		case SQLITE_NULL:
			return .null
		default:
			fatalError("Unknown SQLite column type \(type) encountered for column \(index)")
		}
	}

	/// Returns the value of the column `name`.
	///
	/// - parameter name: The name of the desired column.
	///
	/// - throws: An error if the column`name` doesn't exist.
	///
	/// - returns: The column's value.
	public func value(ofColumn name: String) throws -> Database.Value {
		return try value(ofColumn: statement.index(ofColumn: name))
	}
}

extension Database.Row {
	/// Returns the values of all columns in the row.
	///
	/// - returns: An array of the row's values.
	public func values() throws -> [Database.Value] {
		var values: [Database.Value] = []
		for i in 0 ..< statement.columnCount {
			values.append(try value(ofColumn: i))
		}
		return values
	}

	/// Returns the names and values of all columns in the row.
	///
	/// - warning: This method will fail at runtime if the column names are not unique.
	///
	/// - returns: A dictionary of the row's values keyed by column name.
	public func valueDictionary() throws -> [String: Database.Value] {
		return try Dictionary(uniqueKeysWithValues: statement.columnNames.enumerated().map({ ($0.element, try value(ofColumn: $0.offset)) }))
	}
}

extension Database.Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - parameter index: The index of the desired column.
	///
	/// - returns: The column's value or `nil` if the column doesn't exist.
	public subscript(ofColumn index: Int) -> Database.Value? {
		return try? value(ofColumn: index)
	}

	/// Returns the value of the column with `name`.
	///
	/// - parameter name: The name of the desired column.
	///
	/// - returns: The column's value or `nil` if the column doesn't exist.
	public subscript(ofColumn name: String) -> Database.Value? {
		return try? value(ofColumn: name)
	}
}

extension Database.Row: Collection {
	public var startIndex: Int {
		0
	}

	public var endIndex: Int {
		columnCount
	}

	public subscript(position: Int) -> Database.Value {
		do {
			return try value(ofColumn: position)
		} catch {
			return .null
		}
	}

	public func index(after i: Int) -> Int {
		i + 1
	}
}
