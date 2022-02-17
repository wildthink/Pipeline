//
// Copyright © 2021 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

extension Database {
	/// Executes `sql` with the *n* parameters in `values` bound to the first *n* SQL parameters of `sql` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute.
	/// - parameter values: A collection of values to bind to SQL parameters.
	/// - parameter block: A closure called for each result row.
	/// - parameter row: A result row of returned data.
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `values` couldn't be bound, or the statement couldn't be executed.
	public func execute<C: Collection>(sql: String, parameterValues values: C, _ block: ((_ row: Row) throws -> ())? = nil) throws where C.Element: DatabaseValueConvertible {
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
	public func execute<C: Collection>(sql: String, parameters: C, _ block: ((_ row: Row) throws -> ())? = nil) throws where C.Element == (String, V: DatabaseValueConvertible) {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters: parameters)
		if let block = block {
			try statement.results(block)
		} else {
			try statement.execute()
		}
	}
}
