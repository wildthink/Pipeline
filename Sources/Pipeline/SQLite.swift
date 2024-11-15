//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
@_exported import CSQLite


/// SQLite library information.
public struct SQLite {
	/// The version of SQLite in the format *X.Y.Z*, for example `3.37.2`.
	///
	/// - seealso: [Run-Time Library Version Numbers](https://www.sqlite.org/c3ref/libversion.html)
	public static let version = String(cString: sqlite3_libversion())

	/// The version of SQLite in the format *(X\*1000000 + Y\*1000 + Z)*, such as `3025003`.
	///
	/// - seealso: [Run-Time Library Version Numbers](https://www.sqlite.org/c3ref/libversion.html)
	public static let versionNumber = Int(sqlite3_libversion_number())

	/// The identifier of the SQLite source tree, for example `89e099fbe5e13c33e683bef07361231ca525b88f7907be7092058007b75036f2`.
	///
	/// - seealso: [Run-Time Library Version Numbers](https://www.sqlite.org/c3ref/libversion.html)
	public static let sourceID = String(cString: sqlite3_sourceid())

	/// Initializes the SQLite library.
	///
	/// - throws:  An error if SQLite initialization fails.
	public static func initialize() throws {
		let rc = sqlite3_initialize()
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error initializing sqlite3", code: rc)
		}
	}

	/// Deallocates any resources allocated by `initialize()`.
	///
	/// All open database connections must be closed and all other SQLite resources must be deallocated prior to invoking this function.
	///
	/// - throws:  An error if SQLite shutdown fails.
	public static func shutdown() throws {
		let rc = sqlite3_shutdown()
		guard rc == SQLITE_OK else {
			throw SQLiteError("Error shutting down sqlite3", code: rc)
		}
	}

	/// The number of bytes of memory `malloc`ed but not yet `free`d by SQLite.
	public static var memoryUsed: Int64 {
		sqlite3_memory_used()
	}

	/// Returns the maximum amount of memory used by SQLite since the memory highwater mark was last reset.
	///
	/// - parameter reset: If `true` the memory highwater mark is reset to the value of `memoryUsed`.
	public static func memoryHighwater(reset: Bool = false) -> Int64 {
		sqlite3_memory_highwater(reset ? 1 : 0)
	}

	/// The keywords understood by SQLite.
	///
	/// - note: Keywords in SQLite are not case sensitive.
	///
	/// - seealso: [SQL Keyword Checking](https://www.sqlite.org/c3ref/keyword_check.html)
	public static let keywords: Set<String> = {
		var keywords = Set<String>()
		for i in 0 ..< sqlite3_keyword_count() {
			var chars: UnsafePointer<Int8>?
			var count = Int32(0)
			guard sqlite3_keyword_name(i, &chars, &count) == SQLITE_OK, chars != nil else {
				continue
			}

			let mutableChars = UnsafeMutablePointer(mutating: chars!)
			let data = Data(bytesNoCopy: mutableChars, count: Int(count), deallocator: .none)
			if let keyword = String(data: data, encoding: .utf8) {
				keywords.insert(keyword)
			}
		}
		return keywords
	}()

	/// Tests whether `identifier` is an SQLite keyword.
	///
	/// - parameter identifier: The string to check.
	///
	/// - returns: `true` if `identifier` is an SQLite keyword, `false` otherwise.
	///
	/// - seealso: [SQL Keyword Checking](https://www.sqlite.org/c3ref/keyword_check.html)
	public static func isKeyword(_ identifier: String) -> Bool {
		return identifier.withCString {
			return sqlite3_keyword_check($0, Int32(strlen($0)))
		} != 0
	}

	/// Generates `count` bytes of randomness.
	///
	/// - parameter count: The number of random bytes to generate.
	///
	/// - returns: A `Data` object containing `count` bytes of randomness.
	///
	/// - seealso: [Pseudo-Random Number Generator](https://www.sqlite.org/c3ref/randomness.html)
	public static func randomness(_ count: Int) -> Data {
		var data = Data(count: count)
		data.withUnsafeMutableBytes {
			sqlite3_randomness(Int32($0.count), $0.baseAddress)
		}
		return data
	}

	/// The options defined at compile time.
	///
	/// - seealso: [Run-Time Library Compilation Options Diagnostics](https://sqlite.org/c3ref/compileoption_get.html)
	public static let compileOptions: Set<String> = {
		var options = Set<String>()
		var i: Int32 = 0
		while let option = sqlite3_compileoption_get(i) {
			options.insert(String(cString: option))
			i += 1
		}
		return options
	}()

	/// Tests whether `option` was defined at compile time.
	///
	/// - parameter option: The option to check.
	///
	/// - returns: `true` if `option` was defined at compile time, `false` otherwise.
	///
	/// - seealso: [Run-Time Library Compilation Options Diagnostics](https://sqlite.org/c3ref/compileoption_get.html)
	public static func compileOptionUsed(_ option: String) -> Bool {
		sqlite3_compileoption_used(option) != 0
	}

	/// The content pointer is constant and will never change.
	///
	/// - seealso: [Constants Defining Special Destructor Behavior](https://sqlite.org/c3ref/c_static.html)
	public static let staticStorage = unsafeBitCast(0, to: sqlite3_destructor_type.self)

	/// The content will likely change in the near future and that SQLite should make its own private copy of the content before returning.
	///
	/// - seealso: [Constants Defining Special Destructor Behavior](https://sqlite.org/c3ref/c_static.html)
	public static let transientStorage = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
}
