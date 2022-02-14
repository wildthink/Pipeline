//
// Copyright © 2021 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

extension Database {
	/// A fundamental data type value in an SQLite database.
	///
	/// - seealso: [Datatypes In SQLite](https://sqlite.org/datatype3.html)
	public enum Value {
		/// An integer value.
		case integer(Int64)
		/// A floating-point value.
		case float(Double)
		/// A text value.
		case text(String)
		/// A blob (untyped bytes) value.
		case blob(Data)
		/// A null value.
		case null
	}
}

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
	public func value(forColumn index: Int) throws -> Database.Value {
		guard index >= 0, index < sqlite3_data_count(statement.preparedStatement) else {
			throw Database.Error(message: "Column index \(index) out of bounds")
		}
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
			let f = sqlite3_column_double(statement.preparedStatement, Int32(index))
			guard sqlite3_errcode(statement.database.databaseConnection) == SQLITE_OK else {
				throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
			}
			return .float(f)
		case SQLITE_TEXT:
			let t = String(cString: sqlite3_column_text(statement.preparedStatement, Int32(index)))
			guard sqlite3_errcode(statement.database.databaseConnection) == SQLITE_OK else {
				throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
			}
			return .text(t)
		case SQLITE_BLOB:
			guard let blob = sqlite3_column_blob(statement.preparedStatement, Int32(index)) else {
				guard sqlite3_errcode(statement.database.databaseConnection) == SQLITE_OK else {
					throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
				}
				return .blob(Data())
			}
			let byteCount = Int(sqlite3_column_bytes(statement.preparedStatement, Int32(index)))
			let data = Data(bytes: blob.assumingMemoryBound(to: UInt8.self), count: byteCount)
			return .blob(data)
		case SQLITE_NULL:
			return .null
		default:
			fatalError("Unknown SQLite column type \(type) encountered for column \(index)")
		}
	}

	/// Returns the value of the column at `index` converted to `type`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - parameter index: The index of the desired column.
	/// - parameter type: The desired datatype.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value converted to the desired tyoe.
	public func value(forColumn index: Int, as type: Database.Datatype) throws -> Database.Value {
		guard index >= 0, index < sqlite3_data_count(statement.preparedStatement) else {
			throw Database.Error(message: "Column index \(index) out of bounds")
		}
		switch type {
		case .integer:
			let i = sqlite3_column_int64(statement.preparedStatement, Int32(index))
			guard sqlite3_errcode(statement.database.databaseConnection) == SQLITE_OK else {
				throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
			}
			return .integer(i)
		case .float:
			let f = sqlite3_column_double(statement.preparedStatement, Int32(index))
			guard sqlite3_errcode(statement.database.databaseConnection) == SQLITE_OK else {
				throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
			}
			return .float(f)
		case .text:
			guard let text = sqlite3_column_text(statement.preparedStatement, Int32(index)) else {
				guard sqlite3_errcode(statement.database.databaseConnection) == SQLITE_OK else {
					throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
				}
				// SQL NULL
				return .text("")
			}
			// A String created using String(cString:) from a BLOB containing 0x00 will be shorter than expected
			// but that behavior seems reasonable since type conversion was explicitly requested
			return .text(String(cString: text))
		case .blob:
			guard let blob = sqlite3_column_blob(statement.preparedStatement, Int32(index)) else {
				guard sqlite3_errcode(statement.database.databaseConnection) == SQLITE_OK else {
					throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
				}
				return .blob(Data())
			}
			let byteCount = Int(sqlite3_column_bytes(statement.preparedStatement, Int32(index)))
			let data = Data(bytes: blob.assumingMemoryBound(to: UInt8.self), count: byteCount)
			return .blob(data)
		case .null:
			return .null
		}
	}
}

extension Database.Row {
	/// Returns the values of all columns in the row.
	///
	/// - returns: An array of the row's values.
	public func values() throws -> [Database.Value] {
		var values: [Database.Value] = []
		for i in 0 ..< statement.columnCount {
			values.append(try value(forColumn: i))
		}
		return values
	}

	/// Returns the names and values of all columns in the row.
	///
	/// - warning: This method will fail at runtime if the column names are not unique.
	///
	/// - returns: A dictionary of the row's values keyed by column name.
	public func valueDictionary() throws -> [String: Database.Value] {
		return try Dictionary(uniqueKeysWithValues: statement.columnNames.enumerated().map({ ($0.element, try value(forColumn: $0.offset)) }))
	}
}

extension Database.Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - parameter index: The index of the desired column.
	///
	/// - returns: The column's value or `nil` if null, the column doesn't exist, or contains an illegal value.
	public subscript(forColumn index: Int) -> Database.Value? {
		return try? value(forColumn: index)
	}
}

extension Database.Statement {
	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: Database.Value, toParameter index: Int) throws {
		switch value {
		case .integer(let i):
			guard sqlite3_bind_int64(preparedStatement, Int32(index), i) == SQLITE_OK else {
				throw SQLiteError(fromPreparedStatement: preparedStatement)
			}
		case .float(let f):
			guard sqlite3_bind_double(preparedStatement, Int32(index), f) == SQLITE_OK else {
				throw SQLiteError(fromPreparedStatement: preparedStatement)
			}
		case .text(let s):
			try s.withCString {
				guard sqlite3_bind_text(preparedStatement, Int32(index), $0, -1, SQLiteTransientStorage) == SQLITE_OK else {
					throw SQLiteError(fromPreparedStatement: preparedStatement)
				}
			}
		case .blob(let b):
			try b.withUnsafeBytes {
				guard sqlite3_bind_blob(preparedStatement, Int32(index), $0.baseAddress, Int32(b.count), SQLiteTransientStorage) == SQLITE_OK else {
					throw SQLiteError(fromPreparedStatement: preparedStatement)
				}
			}
		case .null:
			guard sqlite3_bind_null(preparedStatement, Int32(index)) == SQLITE_OK else {
				throw SQLiteError(fromPreparedStatement: preparedStatement)
			}
		}
	}

	/// Binds `value` to the SQL parameter `name`.
	///
	/// - parameter name: The name of the SQL parameter to bind.
	/// - parameter value: The desired value of the SQL parameter.
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `value` couldn't be bound.
	public func bind(_ value: Database.Value, toParameter name: String) throws {
		try bind(value, toParameter: indexOfParameter(named: name))
	}
}

extension Database.Value: Equatable {
	public static func == (lhs: Database.Value, rhs: Database.Value) -> Bool {
		switch (lhs, rhs) {
		case (.integer(let i1), .integer(let i2)):
			return i1 == i2
		case (.float(let f1), .float(let f2)):
			return f1 == f2
		case (.text(let t1), .text(let t2)):
			return t1 == t2
		case (.blob(let b1), .blob(let b2)):
			return b1 == b2
		case (.null, .null):
			// SQL null compares unequal to everything, including null.
			// Is that really the desired behavior here?
			return false
		default:
			return false
		}
	}
}

extension Database.Value: ExpressibleByNilLiteral {
	public init(nilLiteral: ()) {
		self = .null
	}
}

extension Database.Value: ExpressibleByIntegerLiteral {
	public init(integerLiteral value: IntegerLiteralType) {
		self = .integer(Int64(value))
	}
}

extension Database.Value: ExpressibleByFloatLiteral {
	public init(floatLiteral value: FloatLiteralType) {
		self = .float(value)
	}
}

extension Database.Value: ExpressibleByStringLiteral {
	public init(stringLiteral value: StringLiteralType) {
		self = .text(value)
	}
}

extension Database.Value: ExpressibleByBooleanLiteral {
	public init(booleanLiteral value: BooleanLiteralType) {
		self = .integer(value ? 1 : 0)
	}
}

extension Database.Value: CustomStringConvertible {
	/// A description of the type and value of `self`.
	public var description: String {
		switch self {
		case .integer(let i):
			return ".integer(\(i))"
		case .float(let f):
			return ".float(\(f))"
		case .text(let t):
			return ".text('\(t)')"
		case .blob(let b):
			return ".blob(\(b))"
		case .null:
			return ".null"
		}
	}
}
