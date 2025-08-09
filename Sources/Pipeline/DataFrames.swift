//
//  DataFrames.swift
//  Pipeline
//
//  Created by Jason Jobe on 11/12/24.
//

import CoreGraphics
import Foundation
#if TABULAR_DATA_SWIFTPM
import TabularData

// MARK: SQLite Connection
public typealias SQLiteConnection = Connection

extension Connection {
    var ref: OpaquePointer { self.databaseConnection }
    /// Prepares a SQliteStatement from a String.
    ///
    /// The caller is responsible for finalizing the returned SQLiteStatement.
    public func prepare(_ sql: String) throws -> SQLiteStatement {
        try Statement(connection: self, sql: sql)

//        var preparedStatement: SQLiteStatement!
//        try checkSQLite(sqlite3_prepare_v2(ref, statement, -1, &preparedStatement, nil))
//        return preparedStatement
    }
    /// Executes sql statements.
    ///
    /// Wrapper for sqlite3_exec
    public func exec(_ statements: String) throws {
        try checkSQLite(sqlite3_exec(ref, statements, nil, nil, nil))
    }
}

protocol AnyOptional {
    
}

extension Optional: AnyOptional {}

// MARK: SQLite Statement
public typealias SQLiteStatement = Statement

struct _SQLiteStatement {
    var ref: OpaquePointer
}

extension SQLiteStatement {
    var ref: OpaquePointer { self.preparedStatement }
    public func columnType(at col: Int) -> Any.Type {
        columnType(at: Int32(col))
    }
    
    public func columnType(at col: Int32) -> Any.Type {
        guard let declType = sqlite3_column_decltype(ref, Int32(col))
        else { return Any.self }
        
        let typeName = String(cString: declType).uppercased()
        return switch typeName {
            case "INT", "INTEGER": Int64.self
            case "CHAR", "CLOB", "TEXT": String.self
            case "BLOB": Data.self
            case "REAL", "FLOAT", "DOUBLE": Double.self
            case "BOOL": Bool.self
            case "DATE": Date.self
            default: {
                print("WARNING: Unknown SQL Type", typeName)
                return Any.self
            }()
        }
    }
    
    func anyValue(at ndx: Int32) -> Any? {
        sqlite3_column_value(ref, ndx)
    }
    
    func int64Value(at ndx: Int32) -> Int64 {
        sqlite3_column_int64(ref, ndx)
    }
    
    func doubleValue(at ndx: Int32) -> Double {
        sqlite3_column_double(ref, ndx)
    }
    
    func stringValue(at ndx: Int32) -> String {
        (String(cString:sqlite3_column_text(ref, ndx)))
    }
    
    func dataValue(at ndx: Int32) -> Data {
        Data(bytes:sqlite3_column_blob(ref, ndx),
             count:Int(sqlite3_column_bytes(ref, ndx)))
    }
    
    /// Returns true if this step has moved to a new row, false if there are no more rows.
    ///
    /// This is a wrapper for sqlite3_step.
//    public func step()throws -> Bool {
//        return try checkSQLite(sqlite3_step(ref)) == SQLITE_ROW
//    }
    
//    public func reset() throws {
//        try checkSQLite(sqlite3_reset(ref))
//    }
    
    /// A wrapper for sqlite3_finalize.
    //  public func finalize() {
    //    // Ignore the error from sqlite3_finalize, it has already been reported by sqlite3_step.
    ////    sqlite3_finalize(ref)
    //  }
}

// MARK: Extensions
fileprivate let SQLITE_TRANSIENT = unsafeBitCast(OpaquePointer(bitPattern: -1), to: sqlite3_destructor_type.self)


/// The NSError domain for errors thrown from this library.
let kSQLiteDataFrameDomain = "SQLiteDataFrame"

/// Utility method for converting C-style sqlite return codes into Swift errors.
///
/// Example usage:
///
/// ```
///     try checkSQLite(sqlite3_open(":memory:", &db))
/// ```
///
/// The sqlite3 return code is returned as a discardable result.
@discardableResult
func checkSQLite(_ code: Int32) throws -> Int32 {
  if code != SQLITE_OK && code != SQLITE_ROW && code != SQLITE_DONE {
    throw NSError(domain:kSQLiteDataFrameDomain, code:Int(code))
  }
  return code
}

extension Date {
    init?(sqlValue: Any) {
        // See "Date and Time Datatype" https://www.sqlite.org/datatype3.html
        // TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS").
        // REAL as Julian day numbers, the number of days since noon in Greenwich on November 24, 4714 B.C. according
        // to the proleptic Gregorian calendar.
        // INTEGER as Unix Time, the number of seconds since 1970-01-01 00:00:00 UTC.
        //          switch SQLiteValue(statement:statement, columnIndex: columnIndex) {
        guard let it = switch sqlValue {
            case let s as String: {
                let formatter = DateFormatter()
                formatter.dateFormat = "yyyy-MM-dd HH:mm:ss" //this is the sqlite's format
                return formatter.date(from:s)
            }()
            case let i as Int64:
                Date(timeIntervalSince1970:TimeInterval(i))
            case let julianDay as Double: {
                let SECONDS_PER_DAY = 86400.0
                let JULIAN_DAY_OF_ZERO_UNIX_TIME = 2440587.5
                let unixTime = (julianDay - JULIAN_DAY_OF_ZERO_UNIX_TIME) * SECONDS_PER_DAY
                return Date(timeIntervalSince1970:TimeInterval(unixTime))
            }()
            default:
                Optional<Date>.none
        }
        else { return nil }
        self = it
    }
}

/// An enhanced version of the SQLite column type.

public typealias SQLiteType = Any.Type


public extension AnyColumn {
    init(_ name: String, for t: Any.Type, capacity: Int = 0) {
        func mk<T>(_ t: T.Type) -> AnyColumn {
            Column<T>(name: name, capacity: capacity).eraseToAnyColumn()
        }
        self = if let a = t as? AnyOptional.Type {
            _openExistential(a, do: mk)
        } else {
            _openExistential(t, do: mk)
        }
    }
}

extension DataFrame {
    /**
     Intializes a DataFrame from a prepared statement.
     
     - Parameter statement: The prepared statement. The statement will be finalalized by the initializer.
     - Parameter columns: An optional array of column names; Set to nil to use every column in the statement.
     - Parameter types; An optional dictionary of column names to `SQLiteType`s. The data frame infers the types for column names that aren’t in the dictionary.
     - Parameter capacity: The initial capacity of each column. It is normally fine to leave this as the default value.
     
     Columns in the columns parameter which are not returned by the select statement will be ignored.
     The columns parameter is provided for logical consistency with other DataFrame initiializers. However, it is
     inefficent to use this parameter, because the filtering is done after the sql data is fetched from the DB.
     Typically it is more efficient to filter by changing the `statement`.
     
     Columns in the types dictionary which are not returned by the select statement will be ignored.
     
     The DataFrame's column types are determined by the columns' declared types, using a modified version of the
     SQLite3 [Type Affinity](https://www.sqlite.org/datatype3.html) rules.
     If the               column's type can't be determined, then the `.any` type is used.
     */
    public init(
        statement: SQLiteStatement,
        columns: [String]? = nil,
        types: [String:SQLiteType]? = nil,
        capacity: Int = 0
    ) throws {
        
        let columnCount = Int(sqlite3_column_count(statement.ref))
        let columns = (0..<columnCount).map { _ndx in
            let ndx = Int32(_ndx)
            let col_name = String(cString:sqlite3_column_name(statement.ref, ndx))
            let declType = types?[col_name] ?? statement.columnType(at: ndx)
            return AnyColumn(col_name, for: declType)
        }
        self.init(columns: columns)
        try readSQL(statement: statement)
    }
    
    /**
     Read the contents of the given table into this DataFrame.
     
     - Parameter statement: the prepared statement.
     - Parameter finalizeStatement: If true, the prepared statement will be finalized after the read completes.
     
     Columns are matched ito statement parameters n DataFrame column order.
     */
    mutating func readSQL(statement: SQLiteStatement) throws {

        var rowIndex = 0
        while try statement.step() != nil {
            self.appendEmptyRow()
            for (col, column) in columns.enumerated() {
                let columnIndex = Int32(col)
                let sqlColumnType = sqlite3_column_type(statement.ref, columnIndex)
                if sqlColumnType == SQLITE_NULL {
                    continue
                }
                switch column.wrappedElementType {
                    case is Bool.Type:
                        rows[rowIndex][col] = statement.int64Value(at: columnIndex) != 0
                    case is any FixedWidthInteger.Type:
                        if let I = column.wrappedElementType as? any FixedWidthInteger.Type {
                            let iv = I.init(statement.int64Value(at: columnIndex))
                            rows[rowIndex][col] = iv
                        }
                    case is UInt64.Type:
                        if sqlColumnType == SQLITE_TEXT {
                            let iv = UInt64(statement.stringValue(at: columnIndex)) ?? 0
                            rows[rowIndex][col] = iv
                            // This decodes text representation in case its > Int64.max
//                            rows[rowIndex][col] = UInt64(String(cString:sqlite3_column_text(statement.ref, columnIndex))          )
                        } else {
                            rows[rowIndex][col] = UInt64(statement.int64Value(at: columnIndex))
                        }
                    case is String.Type:
                        rows[rowIndex][col] = statement.stringValue(at: columnIndex)
                    case is Float.Type:
                        rows[rowIndex][col] = Float(statement.doubleValue(at: columnIndex))
                    case is Double.Type:
                        rows[rowIndex][col] = Double(statement.doubleValue(at: columnIndex))
                    case is Data.Type:
                        rows[rowIndex][col] = statement.dataValue(at: columnIndex)
                    case is Date.Type:
                        if let date = Date(sqlValue: statement.anyValue(at: columnIndex) as Any) {
                            rows[rowIndex][col] = date
                        }
                    default:
                        if let dt = column.wrappedElementType as? Decodable.Type {
                            let data = statement.dataValue(at: columnIndex)
                            if let it = try? JSONDecoder().decode(dt, from: data) {
                                rows[rowIndex][col] = it
                            }
                        }
                        else if column.wrappedElementType == Any.self,
                            let av = statement.anyValue(at: columnIndex)
                        {
                            rows[rowIndex][col] = av
                        }
                }
            }
            rowIndex += 1
        }
    }
    
    /**
     Write a dataFrame to a sqlite prepared statement.
     - Parameter statement: The prepared statement.
     
     The columns of the dataframe are bound to the statement parameters in column index order.
     
     If there are more dataframe columns than table columns, the extra table columns will be written as null.
     
     If there are more DataFrame columns than table columns, only the first N columns
     will be transferred.
     */
    func writeRows(statement: SQLiteStatement) throws {
        let columns = columns.prefix(Int(sqlite3_bind_parameter_count(statement.ref)))
        for rowIndex in 0..<shape.rows {
            for (i, column) in columns.enumerated() {
                let positionalIndex = Int32(1 + i)
                guard let item = column[rowIndex] else {
                    try checkSQLite(sqlite3_bind_null(statement.ref, positionalIndex))
                    continue
                }
                try DataFrame.writeItem(statement:statement, positionalIndex:positionalIndex, item:item)
            }
            _ = try statement.step()
            try statement.reset()
        }
    }
    
    private static func writeItem(
        statement: SQLiteStatement, positionalIndex: Int32, item: Any
    ) throws {
        func bind_int<I: FixedWidthInteger>(_ v: I) -> Int32 {
            if I.bitWidth <= 32 {
                sqlite3_bind_int(statement.ref, positionalIndex, Int32(v))
            } else {
                sqlite3_bind_int64(statement.ref, positionalIndex, Int64(v))
            }
        }
        switch item {
            case let b as Bool:
                try checkSQLite(bind_int(b ? 1 : 0))
            case let v as any FixedWidthInteger:
                try checkSQLite(bind_int(v))
            case let f as Float:
                try checkSQLite(sqlite3_bind_double(statement.ref, positionalIndex, Double(f)))
            case let f as CGFloat:
                try checkSQLite(sqlite3_bind_double(statement.ref, positionalIndex, Double(f)))
            case let d as Double:
                try checkSQLite(sqlite3_bind_double(statement.ref, positionalIndex, d))
            case let s as String:
                try checkSQLite(sqlite3_bind_text(statement.ref, positionalIndex, s.cString(using: .utf8),-1,SQLITE_TRANSIENT))
            case let d as Data:
                try d.withUnsafeBytes {
                    _ = try checkSQLite(sqlite3_bind_blob64(statement.ref, positionalIndex, $0.baseAddress, sqlite3_uint64($0.count), SQLITE_TRANSIENT))
                }
            case let d as Date:
                let formatter = DateFormatter()
                formatter.dateFormat = "yyyy-MM-dd HH:mm:ss" //this is the sqlite's format.
                let dateString = formatter.string(from: d)
                try checkSQLite(sqlite3_bind_text(statement.ref, positionalIndex, dateString.cString(using: .utf8),-1,SQLITE_TRANSIENT))
            // Backup
            case let cd as Encodable:
                let data = try JSONEncoder().encode(cd)
                try data.withUnsafeBytes {
                    _ = try checkSQLite(sqlite3_bind_blob64(statement.ref, positionalIndex, $0.baseAddress, sqlite3_uint64($0.count), SQLITE_TRANSIENT))
                }
            case let csc as CustomStringConvertible:
                let s = csc.description
                try checkSQLite(sqlite3_bind_text(statement.ref, positionalIndex, s.cString(using: .utf8),-1,SQLITE_TRANSIENT))
            default:
                let s = String(reflecting:item)
                try checkSQLite(sqlite3_bind_text(statement.ref, positionalIndex, s.cString(using: .utf8),-1,SQLITE_TRANSIENT))
        }
        
    }
    
    /**
     Write a dataFrame to a sqlite table.
     - Parameter connection: The SQlite database connection
     - Parameter table: The name of the table to write.
     
     The columns of the dataframe are written to an SQL table. If the table already exists,
     then it will be replaced.
     
     The DataFrame column names and wrapped types will be used to create the
     SQL column names.
     */
    public func writeSQL(connection: SQLiteConnection, table: String, createTable: Bool) throws {
        
        if createTable {
            let columnDefs = columns.map {column -> String in
                let name = column.name
                let sqlType: String? = switch column.wrappedElementType {
                    case is String.Type: "TEXT"
                    case is Bool.Type: "BOOLEAN"
                    case is any FixedWidthInteger: "INT"
                    case is Float.Type: "FLOAT"
                    case is Double.Type: "DOUBLE"
                    case is Date.Type: "DATE"
                    case is Data.Type: "BLOB"
                    default:
                        nil
                }
                if let sqlType = sqlType {
                    return "\(name) \(sqlType)"
                }
                return name
            }
            let columnSpec = columnDefs.joined(separator: ",")
            try connection.exec("create table if not exists \(table) (\(columnSpec))")
        }
        
        let questionMarks = Array(repeating:"?", count:shape.columns).joined(separator: ",")
        let sql = "insert into \(table) values (\(questionMarks))"
        let statement = try connection.prepare(sql)
//        defer { sqlite3_finalize(statement.ref) } Pipeline uses a class.deinit
        try writeRows(statement: statement)
    }
}
#endif // TABULAR_DATA_SWIFTPM
