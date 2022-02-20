//
// Copyright © 2021 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// A type that can be converted to a `Database.Value` for binding to a parameter in an SQLite statement.
///
/// For example, the implementation for `UUID` is:
///
/// ```swift
/// extension UUID: DatabaseValueConvertible {
/// 	public func databaseValue() -> Database.Value {
/// 		return .text(uuidString.lowercased())
/// 	}
/// }
/// ```
public protocol ParameterBindable {
	/// Returns the value of `self` expressed as a `Database.Value`.
	///
	/// - throws: An error if `self` couldn't be converted.
	func databaseValue() throws -> Database.Value
}

extension String: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .text(self)
	}
}

extension Data: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .blob(self)
	}
}

extension Int: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .integer(Int64(self))
	}
}

extension UInt: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .integer(Int64(Int(bitPattern: self)))
	}
}

extension Int8: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .integer(Int64(self))
	}
}

extension UInt8: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .integer(Int64(self))
	}
}

extension Int16: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .integer(Int64(self))
	}
}

extension UInt16: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .integer(Int64(self))
	}
}

extension Int32: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .integer(Int64(self))
	}
}

extension UInt32: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .integer(Int64(self))
	}
}

extension Int64: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .integer(self)
	}
}

extension UInt64: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .integer(Int64(bitPattern: self))
	}
}

extension Float: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .real(Double(self))
	}
}

extension Double: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .real(self)
	}
}

extension Bool: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .integer(self ? 1 : 0)
	}
}

extension UUID: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .text(uuidString.lowercased())
	}
}

extension URL: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .text(absoluteString)
	}
}

extension Date: ParameterBindable {
	public func databaseValue() -> Database.Value {
		return .real(timeIntervalSinceReferenceDate)
	}
}

extension Optional: ParameterBindable where Wrapped: ParameterBindable {
	public func databaseValue() throws -> Database.Value {
		switch self {
		case .none:
			return .null
		case .some(let w):
			return try w.databaseValue()
		}
	}
}
