//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation


/// A comparator for `String` objects.
///
/// - parameter lhs: The left-hand operand.
/// - parameter rhs: The right-hand operand.
///
/// - returns: The result of comparing `lhs` to `rhs`.
public typealias StringComparator = @Sendable (_ lhs: String, _ rhs: String) -> ComparisonResult

extension Connection {
	/// Adds a custom collation function.
	///
	/// ```swift
	/// try connection.addCollation("localizedCompare", { (lhs, rhs) -> ComparisonResult in
	///     return lhs.localizedCompare(rhs)
	/// })
	/// ```
	///
	/// - parameter name: The name of the custom collation sequence.
	/// - parameter block: A string comparison function.
	///
	/// - throws: An error if the collation function couldn't be added.
	public func addCollation(_ name: String, _ block: @escaping StringComparator) throws {
		let function_ptr = UnsafeMutablePointer<StringComparator>.allocate(capacity: 1)
		function_ptr.initialize(to: block)
		let code = sqlite3_create_collation_v2(databaseConnection, name, SQLITE_UTF8, function_ptr, { (context, lhs_len, lhs_data, rhs_len, rhs_data) -> Int32 in
            // Have total faith that SQLite will pass valid parameters and use unsafelyUnwrapped
            if let lhs_data, let rhs_data {
                let lhs = UnsafeBufferPointer(
                    start: lhs_data.assumingMemoryBound(to: UInt8.self),
                    count: Int(lhs_len))
                let l_str = String(decoding: lhs, as: UTF8.self)
                
                let rhs = UnsafeBufferPointer(
                    start: rhs_data.assumingMemoryBound(to: UInt8.self),
                    count: Int(rhs_len))
                let r_str = String(decoding: rhs, as: UTF8.self)

                // Cast context to the appropriate type and call the comparator
                let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: StringComparator.self)
                let result = function_ptr.pointee(l_str, r_str)
                return Int32(result.rawValue)
            } else {
                return SQLITE_ERROR
            }
		}, { context in
			let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: StringComparator.self)
			function_ptr.deinitialize(count: 1)
			function_ptr.deallocate()
        })
        if code != SQLITE_OK {
			throw SQLiteError("Error adding collation sequence \"\(name)\"", takingErrorCodeFromDatabaseConnection: databaseConnection)
		}
	}
    
	/// Removes a custom collation function.
	///
	/// - parameter name: The name of the custom collation sequence.
	///
	/// - throws: An error if the collation function couldn't be removed.
	public func removeCollation(_ name: String) throws {
		guard sqlite3_create_collation_v2(databaseConnection, name, SQLITE_UTF8, nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error removing collation sequence \"\(name)\"", takingErrorCodeFromDatabaseConnection: databaseConnection)
		}
	}
}
