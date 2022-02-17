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
	public func bind<T: DatabaseValueConvertible>(value: T, toParameter index: Int) throws {
		try bind(value: value.databaseValue(), toParameter: index)
	}

	/// Binds `value` to the SQL parameter `name`.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter name: The name of the SQL parameter to bind.
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `value` couldn't be bound.
	public func bind<T: DatabaseValueConvertible>(value: T, toParameter name: String) throws {
		try bind(value: value.databaseValue(), toParameter: indexOfParameter(named: name))
	}
}

extension Database.Statement {
	/// Binds the *n* parameters in `values` to the first *n* SQL parameters of `self`.
	///
	/// - parameter values: A collection of values to bind to SQL parameters.
	///
	/// - throws: An error if one of `values` couldn't be bound.
	public func bind<C: Collection>(parameterValues values: C) throws where C.Element: DatabaseValueConvertible {
		var index = 1
		for value in values {
			try bind(value: value.databaseValue(), toParameter: index)
			index += 1
		}
	}

	/// Binds *value* to SQL parameter *name* for each (*name*, *value*) in `parameters`.
	///
	/// - parameter parameters: A collection of name and value pairs to bind to SQL parameters.
	///
	/// - throws: An error if the SQL parameter *name* doesn't exist or *value* couldn't be bound.
	///
	/// - returns: `self`
	public func bind<C: Collection>(parameters: C) throws where C.Element == (String, V: DatabaseValueConvertible) {
		for (name, value) in parameters {
			try bind(value: value.databaseValue(), toParameter: indexOfParameter(named: name))
		}
	}
}
