//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// A type that can bind its value to a parameter in an SQLite statement.
///
/// For example, the implementation for `UUID` is:
///
/// ```swift
/// extension UUID: ParameterBindable {
/// 	public func bind(toStatement statement: Statement, parameter index: Int) throws {
/// 		try statement.bind(text: uuidString.lowercased(), toParameter: index)
/// 	}
/// }
/// ```
public protocol ParameterBindable {
	/// Binds the value of `self` to the SQL parameter at `index` in `statement`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - parameter statement: A `Statement` object,
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `index` is out of bounds or `self` couldn't be bound.
	func bind(toStatement statement: Statement, parameter index: Int) throws
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
	public func bind<T: ParameterBindable>(_ value: T, toParameter index: Int) throws {
		try value.bind(toStatement: self, parameter: index)
	}

	/// Binds `value` to the SQL parameter `name`.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter name: The name of the SQL parameter to bind.
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `value` couldn't be bound.
	public func bind<T: ParameterBindable>(_ value: T, toParameter name: String) throws {
		try bind(value, toParameter: indexOfParameter(named: name))
	}
}

extension Statement {
	/// Binds the *n* parameters in `values` to the first *n* SQL parameters of `self`.
	///
	/// - requires: `values.count <= self.parameterCount`
	///
	/// - parameter values: A collection of values to bind to SQL parameters.
	///
	/// - throws: An error if one of `values` couldn't be bound.
	///
	/// - returns: `self`
	@discardableResult public func bind<C: Collection>(parameterValues values: C) throws -> Statement where C.Element: ParameterBindable {
		var index = 1
		for value in values {
			try value.bind(toStatement: self, parameter: index)
			index += 1
		}
		return self
	}

	/// Binds *value* to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - requires: `parameters.count <= self.parameterCount`
	///
	/// - parameter parameters: A collection of name and value pairs to bind to SQL parameters.
	///
	/// - throws: An error if the SQL parameter *name* doesn't exist or *value* couldn't be bound.
	///
	/// - returns: `self`
	@discardableResult public func bind<C: Collection>(parameters: C) throws -> Statement where C.Element == (String, V: ParameterBindable) {
		for (name, value) in parameters {
			try value.bind(toStatement: self, parameter: indexOfParameter(named: name))
		}
		return self
	}
}

extension Statement {
	/// Binds the *n* parameters in `values` to the first *n* SQL parameters of `self`.
	///
	/// - requires: `values.count <= self.parameterCount`
	///
	/// - parameter values: A series of values to bind to SQL parameters.
	///
	/// - throws: An error if one of `values` couldn't be bound.
	///
	/// - returns: `self`
	@discardableResult public func bind(parameterValues values: [ParameterBindable]) throws -> Statement {
		var index = 1
		for value in values {
			try value.bind(toStatement: self, parameter: index)
			index += 1
		}
		return self
	}

	/// Binds *value* to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - parameter parameters: A series of name and value pairs to bind to SQL parameters
	///
	/// - throws: An error if the SQL parameter *name* doesn't exist or *value* couldn't be bound
	///
	/// - returns: `self`
	@discardableResult public func bind(parameters: [String: ParameterBindable]) throws -> Statement {
		for (name, value) in parameters {
			try value.bind(toStatement: self, parameter: indexOfParameter(named: name))
		}
		return self
	}
}

extension Database {
	/// Executes `sql` with the *n* parameters in `values` bound to the first *n* SQL parameters of `sql` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute.
	/// - parameter values: A collection of values to bind to SQL parameters.
	/// - parameter block: A closure called for each result row.
	/// - parameter row: A result row of returned data.
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `values` couldn't be bound, or the statement couldn't be executed.
	public func execute<C: Collection>(sql: String, parameterValues values: C, _ block: ((_ row: Row) throws -> ())? = nil) throws where C.Element: ParameterBindable {
		let statement = try prepare(sql: sql)
		try statement.bind(parameterValues: values)
		if let block = block {
			try statement.results(block)
		} else {
			try statement.execute()
		}
	}

	/// Executes `sql` with *value* bound to SQL parameter *name* for each (*name*, *value*) in `parameters` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute.
	/// - parameter parameters: A collection of name and value pairs to bind to SQL parameters.
	/// - parameter block: A closure called for each result row.
	/// - parameter row: A result row of returned data.
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `parameters` couldn't be bound, or the statement couldn't be executed.
	public func execute<C: Collection>(sql: String, parameters: C, _ block: ((_ row: Row) throws -> ())? = nil) throws where C.Element == (String, V: ParameterBindable) {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: parameters)
		if let block = block {
			try statement.results(block)
		} else {
			try statement.execute()
		}
	}
}

extension String: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(text: self, toParameter: index)
	}
}

extension Data: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(blob: self, toParameter: index)
	}
}

extension Int: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(integer: Int64(self), toParameter: index)
	}
}

extension UInt: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(integer: Int64(Int(bitPattern: self)), toParameter: index)
	}
}

extension Int8: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(integer: Int64(self), toParameter: index)
	}
}

extension UInt8: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(integer: Int64(self), toParameter: index)
	}
}

extension Int16: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(integer: Int64(self), toParameter: index)
	}
}

extension UInt16: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(integer: Int64(self), toParameter: index)
	}
}

extension Int32: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(integer: Int64(self), toParameter: index)
	}
}

extension UInt32: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(integer: Int64(self), toParameter: index)
	}
}

extension Int64: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(integer: self, toParameter: index)
	}
}

extension UInt64: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(integer: Int64(bitPattern: self), toParameter: index)
	}
}

extension Float: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(real: Double(self), toParameter: index)
	}
}

extension Double: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(real: self, toParameter: index)
	}
}

extension Bool: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(integer: self ? 1 : 0, toParameter: index)
	}
}

extension UUID: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(text: uuidString.lowercased(), toParameter: index)
	}
}

extension URL: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(text: absoluteString, toParameter: index)
	}
}

extension Date: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		try statement.bind(real: timeIntervalSinceReferenceDate, toParameter: index)
	}
}

extension Optional: ParameterBindable where Wrapped: ParameterBindable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		switch self {
		case .none:
			try statement.bindNull(toParameter: index)
		case .some(let w):
			try w.bind(toStatement: statement, parameter: index)
		}
	}
}

extension ParameterBindable where Self: Encodable {
	public func bind(toStatement statement: Statement, parameter index: Int) throws {
		let data = try JSONEncoder().encode(self)
		try statement.bind(blob: data, toParameter: index)
	}
}
