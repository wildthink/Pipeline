//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation


extension Statement {
	/// The number of SQL parameters in this statement.
	public var parameterCount: Int {
		Int(sqlite3_bind_parameter_count(preparedStatement))
	}

	/// Returns the name of the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - parameter index: The index of the desired SQL parameter.
	///
	/// - returns: The name of the specified parameter.
	public func nameOfParameter(_ index: Int) throws -> String {
		guard let name = sqlite3_bind_parameter_name(preparedStatement, Int32(index)) else {
			throw DatabaseError("SQL parameter at index \(index) not found or nameless")
		}
		return String(cString: name)
	}

	/// Returns the index of the SQL parameter with `name`.
	///
	/// - parameter name: The name of the desired SQL parameter.
	///
	/// - returns: The index of the specified parameter.
	public func indexOfParameter(_ name: String) throws -> Int {
		let index = sqlite3_bind_parameter_index(preparedStatement, name)
		guard index != 0 else {
			throw DatabaseError("SQL parameter \"\(name)\" not found")
		}
		return Int(index)
	}

	/// Clears all statement bindings by setting SQL parameters to null.
	///
	/// - throws: An error if the bindings could not be cleared.
	///
	/// - returns: `self`.
	@discardableResult public func clearBindings() throws -> Statement {
		guard sqlite3_clear_bindings(preparedStatement) == SQLITE_OK else {
			throw SQLiteError("Error clearing bindings", takingErrorCodeFromDatabaseConnection: connection.databaseConnection)
		}
		return self
	}
}

extension Statement {
	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind(value: DatabaseValue, toParameter index: Int) throws -> Statement {
		switch value {
		case .integer(let i):
			guard sqlite3_bind_int64(preparedStatement, Int32(index), i) == SQLITE_OK else {
				throw SQLiteError("Error binding Int64 \(i) to parameter \(index)", takingErrorCodeFromDatabaseConnection: connection.databaseConnection)
			}
		case .real(let r):
			guard sqlite3_bind_double(preparedStatement, Int32(index), r) == SQLITE_OK else {
				throw SQLiteError("Error binding Double \(r) to parameter \(index)", takingErrorCodeFromDatabaseConnection: connection.databaseConnection)
			}
		case .text(let t):
			try t.withCString {
				guard sqlite3_bind_text(preparedStatement, Int32(index), $0, -1, SQLite.transientStorage) == SQLITE_OK else {
					throw SQLiteError("Error binding String \"\(t)\" to parameter \(index)", takingErrorCodeFromDatabaseConnection: connection.databaseConnection)
				}
			}
		case .blob(let b):
			try b.withUnsafeBytes {
				guard sqlite3_bind_blob(preparedStatement, Int32(index), $0.baseAddress, Int32($0.count), SQLite.transientStorage) == SQLITE_OK else {
					throw SQLiteError("Error binding Data to parameter \(index)", takingErrorCodeFromDatabaseConnection: connection.databaseConnection)
				}
			}
		case .null:
			guard sqlite3_bind_null(preparedStatement, Int32(index)) == SQLITE_OK else {
				throw SQLiteError("Error binding null to parameter \(index)", takingErrorCodeFromDatabaseConnection: connection.databaseConnection)
			}
		}
		return self
	}

	/// Binds `value` to the SQL parameter `name`.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter name: The name of the SQL parameter to bind.
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind(value: DatabaseValue, toParameter name: String) throws -> Statement {
		try bind(value: value, toParameter: indexOfParameter(name))
	}
}

extension Statement {
	/// Binds the signed integer `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind(integer value: Int64, toParameter index: Int) throws -> Statement {
		guard sqlite3_bind_int64(preparedStatement, Int32(index), value) == SQLITE_OK else {
			throw SQLiteError("Error binding Int64 \(value) to parameter \(index)", takingErrorCodeFromDatabaseConnection: connection.databaseConnection)
		}
		return self
	}

	/// Binds the floating-point `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind(real value: Double, toParameter index: Int) throws -> Statement {
		guard sqlite3_bind_double(preparedStatement, Int32(index), value) == SQLITE_OK else {
			throw SQLiteError("Error binding Double \(value) to parameter \(index)", takingErrorCodeFromDatabaseConnection: connection.databaseConnection)
		}
		return self
	}

	/// Binds the text `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind(text value: String, toParameter index: Int) throws -> Statement {
		try value.withCString {
			guard sqlite3_bind_text(preparedStatement, Int32(index), $0, -1, SQLite.transientStorage) == SQLITE_OK else {
				throw SQLiteError("Error binding String \(value) to parameter \(index)", takingErrorCodeFromDatabaseConnection: connection.databaseConnection)
			}
		}
		return self
	}

	/// Binds the BLOB `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind(blob value: Data, toParameter index: Int) throws -> Statement {
		try value.withUnsafeBytes {
			guard sqlite3_bind_blob(preparedStatement, Int32(index), $0.baseAddress, Int32($0.count), SQLite.transientStorage) == SQLITE_OK else {
				throw SQLiteError("Error binding Data to parameter \(index)", takingErrorCodeFromDatabaseConnection: connection.databaseConnection)
			}
		}
		return self
	}

	/// Binds an SQL `NULL` value to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if SQL `NULL` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bindNull(toParameter index: Int) throws -> Statement {
		guard sqlite3_bind_null(preparedStatement, Int32(index)) == SQLITE_OK else {
			throw SQLiteError("Error binding null to parameter \(index)", takingErrorCodeFromDatabaseConnection: connection.databaseConnection)
		}
		return self
	}
}

extension Statement {
	/// Binds the signed integer `value` to the SQL parameter `name`.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter name: The name of the SQL parameter to bind.
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind(integer value: Int64, toParameter name: String) throws -> Statement {
		try bind(integer: value, toParameter: indexOfParameter(name))
	}

	/// Binds the floating-point `value` to the SQL parameter `name`.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter name: The name of the SQL parameter to bind.
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind(real value: Double, toParameter name: String) throws -> Statement {
		try bind(real: value, toParameter: indexOfParameter(name))
	}

	/// Binds the text `value` to the SQL parameter `name`.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter name: The name of the SQL parameter to bind.
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind(text value: String, toParameter name: String) throws -> Statement {
		try bind(text: value, toParameter: indexOfParameter(name))
	}

	/// Binds the BLOB `value` to the SQL parameter `name`.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter name: The name of the SQL parameter to bind.
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `value` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bind(blob value: Data, toParameter name: String) throws -> Statement {
		try bind(blob: value, toParameter: indexOfParameter(name))
	}

	/// Binds an SQL `NULL` value to the SQL parameter `name`.
	///
	/// - parameter name: The name of the SQL parameter to bind.
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or SQL `NULL` couldn't be bound.
	///
	/// - returns: `self`.
	@discardableResult public func bindNull(toParameter name: String) throws -> Statement {
		try bindNull(toParameter: indexOfParameter(name))
	}
}
