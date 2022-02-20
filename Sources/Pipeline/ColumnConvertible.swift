//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// A type that can be initialized from a value in an SQLite result row.
///
/// The implementation normally uses either `integer(forColumn:)`, `real(forColumn:)`,
/// `text(forColumn:)`, or`blob(forColumn:)` but lower-level SQLite
/// operations are also possible.
///
/// For example, the implementation for `UUID` is:
///
/// ```swift
/// extension UUID: ColumnConvertible {
///     public init(row: Row, column index: Int) throws {
/// 		let s = try row.text(forColumn: index)
/// 		guard let u = UUID(uuidString: s) else {
/// 			throw Database.Error(message: "text \"\(s)\" isn't a valid UUID")
/// 		}
/// 		self = u
///     }
/// }
///  ```
public protocol ColumnConvertible {
	/// Creates an instance containing the value of column `index` in `row`.
	///
	/// - precondition: `row.type(ofColumn: index) != .null`
	///
	/// - parameter row: A `Row` object containing the desired value.
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if the column contains an illegal value or initialization fails.
	init(row: Row, column index: Int) throws
}

extension Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds or the column contains an illegal value.
	///
	/// - returns: The column's value or `nil` if null
	public func value<T: ColumnConvertible>(forColumn index: Int) throws -> T? {
		if try type(ofColumn: index) == .null {
			return nil
		}
		return try T(row: self, column: index)
	}

	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds or the column contains a null or illegal value.
	///
	/// - returns: The column's value.
	public func value<T: ColumnConvertible>(forColumn index: Int) throws -> T {
		guard let value: T = try value(forColumn: index) else {
			throw Database.Error(message: "SQL NULL encountered for column \(index)")
		}
		return value
	}

	/// Returns the value of the column with name `name`.
	///
	/// - parameter name: The name of the desired column.
	///
	/// - throws: An error if the column doesn't exist or contains an illegal value.
	///
	/// - returns: The column's value or `nil` if null.
	public func value<T: ColumnConvertible>(forColumn name: String) throws -> T? {
		return try value(forColumn: statement.index(ofColumn: name))
	}

	/// Returns the value of the column with name `name`.
	///
	/// - parameter name: The name of the desired column.
	///
	/// - throws: An error if the column doesn't exist or contains a null or illegal value.
	///
	/// - returns: The column's value.
	public func value<T: ColumnConvertible>(forColumn name: String) throws -> T {
		return try value(forColumn: statement.index(ofColumn: name))
	}
}

extension String: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self = try row.text(forColumn: index)
	}
}

extension Data: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self = try row.blob(forColumn: index)
	}
}

extension Int: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self.init(try row.integer(forColumn: index))
	}
}

extension UInt: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self.init(bitPattern: Int(try row.integer(forColumn: index)))
	}
}

extension Int8: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self.init(try row.integer(forColumn: index))
	}
}

extension UInt8: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self.init(try row.integer(forColumn: index))
	}
}

extension Int16: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self.init(try row.integer(forColumn: index))
	}
}

extension UInt16: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self.init(try row.integer(forColumn: index))
	}
}

extension Int32: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self.init(try row.integer(forColumn: index))
	}
}

extension UInt32: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self.init(try row.integer(forColumn: index))
	}
}

extension Int64: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self = try row.integer(forColumn: index)
	}
}

extension UInt64: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self.init(bitPattern: try row.integer(forColumn: index))
	}
}

extension Float: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self.init(try row.real(forColumn: index))
	}
}

extension Double: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self = try row.real(forColumn: index)
	}
}

extension Bool: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self.init(try row.integer(forColumn: index) != 0)
	}
}

extension UUID: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		let s = try row.text(forColumn: index)
		guard let u = UUID(uuidString: s) else {
			throw Database.Error(message: "text \"\(s)\" isn't a valid UUID")
		}
		self = u
	}
}

extension URL: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		let s = try row.text(forColumn: index)
		guard let u = URL(string: s) else {
			throw Database.Error(message: "text \"\(s)\" isn't a valid URL")
		}
		self = u
	}
}

extension Date: ColumnConvertible {
	public init(row: Row, column index: Int) throws {
		self.init(timeIntervalSinceReferenceDate: try row.real(forColumn: index))
	}
}
