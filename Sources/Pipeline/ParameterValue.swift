//
// Copyright © 2015 - 2022 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import CSQLite

/// A struct responsible for binding a value to an SQL parameter.
///
/// The implementation normally uses either `bind(integer:toParameter:)`, `bind(real:toParameter:)`,
/// `bind(text:toParameter:)`, or`bind(blob:toParameter:)` but lower-level SQLite
/// operations are also possible.
///
/// For example, an implementation for binding a `UUID` as text is:
///
/// ```swift
/// extension ParameterValue {
/// 	public static func uuidString(_ value: UUID) -> ParameterValue {
///			ParameterValue { statement, index in
///				try statement.bind(text: value.uuidString.lowercased(), toParameter: index)
/// 		}
/// 	}
///  ```
public struct ParameterValue {
	/// Binds a value to the SQL parameter at `index` in `statement`.
	///
	/// - parameter statement: A `Statement` object to receive the desired parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if the value could not be bound.
	public let bind: (_ statement: Statement, _ index: Int) throws -> ()
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
	public func bind(_ value: ParameterValue, toParameter index: Int) throws {
		try value.bind(self, index)
	}

	/// Binds `value` to the SQL parameter `name`.
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter name: The name of the SQL parameter to bind.
	///
	/// - throws: An error if the SQL parameter `name` doesn't exist or `value` couldn't be bound.
	public func bind(_ value: ParameterValue, toParameter name: String) throws {
		try value.bind(self, indexOfParameter(name))
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
	@discardableResult public func bind<C: Collection>(_ values: C) throws -> Statement where C.Element == ParameterValue {
		var index = 1
		for value in values {
			try value.bind(self, index)
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
	@discardableResult public func bind<C: Collection>(_ parameters: C) throws -> Statement where C.Element == (key: String, value: ParameterValue) {
		for (name, value) in parameters {
			try value.bind(self, indexOfParameter(name))
		}
		return self
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
	@discardableResult public func bind(_ values: ParameterValue...) throws -> Statement {
		try bind(values)
	}
}

extension Database {
	/// Executes `sql` with the *n* `parameters` bound to the first *n* SQL parameters of `sql` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute.
	/// - parameter parameters: A collection of values to bind to SQL parameters.
	/// - parameter block: A closure called for each result row.
	/// - parameter row: A result row of returned data.
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `values` couldn't be bound, or the statement couldn't be executed.
	public func execute<C: Collection>(sql: String, parameters: C, _ block: ((_ row: Row) throws -> ())? = nil) throws where C.Element == ParameterValue {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters)
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
	public func execute<C: Collection>(sql: String, parameters: C, _ block: ((_ row: Row) throws -> ())? = nil) throws where C.Element == (key: String, value: ParameterValue) {
		let statement = try prepare(sql: sql)
		try statement.bind(parameters)
		if let block = block {
			try statement.results(block)
		} else {
			try statement.execute()
		}
	}
}

extension Database {
	/// Executes `sql` with the *n* `parameters` bound to the first *n* SQL parameters of `sql` and applies `block` to each result row.
	///
	/// - parameter sql: The SQL statement to execute.
	/// - parameter parameters: A series of values to bind to SQL parameters.
	/// - parameter block: A closure called for each result row.
	/// - parameter row: A result row of returned data.
	///
	/// - throws: Any error thrown in `block` or an error if `sql` couldn't be compiled, `values` couldn't be bound, or the statement couldn't be executed.
	public func execute(sql: String, parameters: ParameterValue..., block: ((_ row: Row) throws -> ())? = nil) throws {
		try execute(sql: sql, parameters: parameters, block)
	}
}

extension ParameterValue {
	/// Binds a text value.
	public static func string(_ value: String) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(text: value, toParameter: index)
		}
	}

	/// Binds a text value.
	public static let text = string
}

extension ParameterValue {
	/// Binds a BLOB value.
	public static func data(_ value: Data) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(blob: value, toParameter: index)
		}
	}

	/// Binds a BLOB value.
	public static let blob = data
}

extension ParameterValue {
	/// Binds a SQL NULL value.
	public static let null = ParameterValue { statement, index in
		try statement.bindNull(toParameter: index)
	}
}

extension ParameterValue {
	/// Binds an `Int` as a signed integer.
	public static func int(_ value: Int) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(integer: Int64(value), toParameter: index)
		}
	}

	/// Binds a `UInt` as a signed integer.
	/// - note: The value is bound as an `Int` bit pattern.
	public static func uint(_ value: UInt) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(integer: Int64(Int(bitPattern: value)), toParameter: index)
		}
	}

	/// Binds an `Int8` as a signed integer.
	public static func int8(_ value: Int8) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(integer: Int64(value), toParameter: index)
		}
	}

	/// Binds a `UInt8` as a signed integer.
	public static func uint8(_ value: UInt8) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(integer: Int64(value), toParameter: index)
		}
	}

	/// Binds an `Int16` as a signed integer.
	public static func int16(_ value: Int16) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(integer: Int64(value), toParameter: index)
		}
	}

	/// Binds a `UInt16` as a signed integer.
	public static func uint16(_ value: UInt16) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(integer: Int64(value), toParameter: index)
		}
	}

	/// Binds an `Int32` as a signed integer.
	public static func int32(_ value: Int32) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(integer: Int64(value), toParameter: index)
		}
	}

	/// Binds a `UInt32` as a signed integer.
	public static func uint32(_ value: UInt32) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(integer: Int64(value), toParameter: index)
		}
	}

	/// Binds an `Int64` as a signed integer.
	public static func int64(_ value: Int64) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(integer: value, toParameter: index)
		}
	}

	/// Binds a signed integer value.
	public static let integer = int64

	/// Binds a `UInt64` as a signed integer.
	/// - note: The value is bound as an `Int64` bit pattern.
	public static func uint64(_ value: UInt64) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(integer: Int64(bitPattern: value), toParameter: index)
		}
	}
}

extension ParameterValue {
	/// Binds an `Float` as a floating-point value.
	public static func float(_ value: Float) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(real: Double(value), toParameter: index)
		}
	}

	/// Binds an `Double` as a floating-point value.
	public static func double(_ value: Double) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(real: value, toParameter: index)
		}
	}

	/// Binds a floating-point value.
	public static let real = double
}

extension ParameterValue {
	/// Binds a `Bool` as a signed integer.
	/// - note: True is bound as 1 while false is bound as 0.
	public static func bool(_ value: Bool) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(integer: value ? 1 : 0, toParameter: index)
		}
	}
}

extension ParameterValue {
	/// Binds a `UUID` as text.
	/// - note: The value is bound as a lower case UUID string.
	public static func uuidString(_ value: UUID) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(text: value.uuidString.lowercased(), toParameter: index)
		}
	}

	/// Binds a `UUID` as a BLOB.
	/// - note: The value is bound as a 16-byte `uuid_t`.
	public static func uuidBytes(_ value: UUID) -> ParameterValue {
		ParameterValue { statement, index in
			let b = withUnsafeBytes(of: value.uuid) {
				Data($0)
			}
			try statement.bind(blob: b, toParameter: index)
		}
	}
}

extension ParameterValue {
	/// Binds a `URL` as text.
	public static func urlString(_ value: URL) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(text: value.absoluteString, toParameter: index)
		}
	}
}

extension ParameterValue {
	/// Binds a `Date` as a floating-point value.
	/// - note: The value is bound as the number of seconds relative to 00:00:00 UTC on 1 January 1970.
	public static func timeIntervalSince1970(_ value: Date) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(real: value.timeIntervalSince1970, toParameter: index)
		}
	}

	/// Binds a `Date` as a floating-point value.
	/// - note: The value is bound as the number of seconds relative to 00:00:00 UTC on 1 January 2001.
	public static func timeIntervalSinceReferenceDate(_ value: Date) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(real: value.timeIntervalSinceReferenceDate, toParameter: index)
		}
	}

	/// Binds a `Date` as a text value.
	/// - parameter formatter: The formatter to use to generate the ISO 8601 date representation.
	public static func iso8601DateString(_ value: Date, formatter: ISO8601DateFormatter = ISO8601DateFormatter()) -> ParameterValue {
		ParameterValue { statement, index in
			try statement.bind(text	: formatter.string(from: value), toParameter: index)
		}
	}
}

extension ParameterValue {
	/// Binds an `Encodable` instance as encoded JSON data.
	/// - parameter encoder: The encoder to use to generate the encoded JSON data.
	public static func json<T>(_ value: T, encoder: JSONEncoder = JSONEncoder()) -> ParameterValue where T: Encodable {
		return ParameterValue { statement, index in
			let b = try encoder.encode(value)
			try statement.bind(blob: b, toParameter: index)
		}
	}
}

extension ParameterValue: ExpressibleByNilLiteral {
	/// Binds a SQL NULL value.
	public init(nilLiteral: ()) {
		self = .null
	}
}

extension ParameterValue: ExpressibleByIntegerLiteral {
	/// Binds a signed integer value.
	public init(integerLiteral value: IntegerLiteralType) {
		self = .integer(Int64(value))
	}
}

extension ParameterValue: ExpressibleByFloatLiteral {
	/// Binds a floating-point value.
	public init(floatLiteral value: FloatLiteralType) {
		self = .real(value)
	}
}

extension ParameterValue: ExpressibleByStringLiteral {
	/// Binds a text value.
	public init(stringLiteral value: StringLiteralType) {
		self = .text(value)
	}
}

extension ParameterValue: ExpressibleByBooleanLiteral {
	/// Binds a boolean value as a signed integer.
	/// - note: True is bound as 1 while false is bound as 0.
	public init(booleanLiteral value: BooleanLiteralType) {
		self = .integer(value ? 1 : 0)
	}
}

extension ParameterValue {
	/// Binds an `NSNumber` as a signed integer or floating-point value.
	public static func nsNumber(_ value: NSNumber) -> ParameterValue {
		ParameterValue { statement, index in
			switch CFNumberGetType(value as CFNumber) {
			case .sInt8Type, .sInt16Type, .sInt32Type, .charType, .shortType, .intType,
					.sInt64Type, .longType, .longLongType, .cfIndexType, .nsIntegerType:
				try statement.bind(integer: value.int64Value, toParameter: index)
			case .float32Type, .float64Type, .floatType, .doubleType, .cgFloatType:
				try statement.bind(real: value.doubleValue, toParameter: index)
			@unknown default:
				fatalError("Unexpected CFNumber type")
			}
		}
	}
}

extension ParameterValue {
	/// Binds an `NSCoding` instance as keyed archive data using `NSKeyedArchiver`.
	public static func nsKeyedArchive<T>(_ value: T) -> ParameterValue where T: NSObject, T: NSCoding {
		ParameterValue { statement, index in
			let b = try NSKeyedArchiver.archivedData(withRootObject: value, requiringSecureCoding: true)
			try statement.bind(blob: b, toParameter: index)
		}
	}
}
