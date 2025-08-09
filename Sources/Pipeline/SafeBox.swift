//
//  SafeBox.swift
//  Pipeline
//
//  Created by Jason Jobe on 8/9/25.
//


import Foundation

@propertyWrapper
public struct SafeBox<Value>: @unchecked Sendable {
    private let storage: Storage
    
    public init(wrappedValue: Value) { self.storage = Storage(wrappedValue) }
    
    public var wrappedValue: Value {
        get { storage.lock.lock(); defer { storage.lock.unlock() }; return storage.value }
        set { storage.lock.lock(); storage.value = newValue; storage.lock.unlock() }
    }
    
    public var projectedValue: SafeBox<Value> { self }
    
    @discardableResult
    public func with<T>(_ body: (inout Value) throws -> T) rethrows -> T {
        storage.lock.lock(); defer { storage.lock.unlock() }
        return try body(&storage.value)
    }
    final class Storage {
        let lock = NSLock()
        var value: Value
        init(_ value: Value) { self.value = value }
    }

//
//    @inlinable public func get() -> Value { wrappedValue }
//    @inlinable public func set(_ newValue: Value) { wrappedValue = newValue }
}

// Example
func testSafeBox() {
    @SafeBox var data = [Int]()
    $data.with { $0.append(contentsOf: [1,2,3]) }
    let snapshot = data
    print(snapshot)
}
