//
// Copyright © 2021 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

extension Database.Statement {
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
	public func nameOfParameter(atIndex index: Int) throws -> String {
		guard let name = sqlite3_bind_parameter_name(preparedStatement, Int32(index)) else {
			throw Database.Error(message: "SQL parameter at index \(index) not found or nameless")
		}
		return String(cString: name)
	}

	/// Returns the index of the SQL parameter with `name`.
	///
	/// - parameter name: The name of the desired SQL parameter.
	///
	/// - returns: The index of the specified parameter.
	public func indexOfParameter(named name: String) throws -> Int {
		let index = sqlite3_bind_parameter_index(preparedStatement, name)
		guard index != 0 else {
			throw Database.Error(message: "SQL parameter \"\(name)\" not found")
		}
		return Int(index)
	}

	/// Clears all statement bindings by setting SQL parameters to null.
	///
	/// - throws: An error if the bindings could not be cleared.
	public func clearBindings() throws {
		guard sqlite3_clear_bindings(preparedStatement) == SQLITE_OK else {
			throw SQLiteError(fromDatabaseConnection: database.databaseConnection)
		}
	}
}
