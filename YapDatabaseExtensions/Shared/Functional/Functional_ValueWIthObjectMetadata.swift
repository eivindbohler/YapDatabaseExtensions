//
//  Functional_ObjectWithNoMetadata.swift
//  YapDatabaseExtensions
//
//  Created by Daniel Thorpe on 11/10/2015.
//
//

import Foundation
import ValueCoding
import YapDatabase

// MARK: - Reading

extension ReadTransactionType {

    /**
    Reads the item at a given index.

    - parameter index: a YapDB.Index
    - returns: an optional `ItemType`
    */
    public func readAtIndex<
        ValueWithObjectMetadata>(_ index: YapDB.Index) -> ValueWithObjectMetadata? where
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            if var item = ValueWithObjectMetadata.decode(readAtIndex(index)) {
                item.metadata = readMetadataAtIndex(index)
                return item
            }
            return nil
    }

    /**
    Reads the items at the indexes.

    - parameter indexes: a SequenceType of YapDB.Index values
    - returns: an array of `ItemType`
    */
    public func readAtIndexes<
        Indexes, ValueWithObjectMetadata>(_ indexes: Indexes) -> [ValueWithObjectMetadata] where
        Indexes: Sequence,
        Indexes.Iterator.Element == YapDB.Index,
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            return indexes.flatMap(readAtIndex)
    }

    /**
    Reads the item at the key.

    - parameter key: a String
    - returns: an optional `ItemType`
    */
    public func readByKey<
        ValueWithObjectMetadata>(_ key: String) -> ValueWithObjectMetadata? where
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            return readAtIndex(ValueWithObjectMetadata.indexWithKey(key))
    }

    /**
    Reads the items by the keys.

    - parameter keys: a SequenceType of String values
    - returns: an array of `ItemType`
    */
    public func readByKeys<
        Keys, ValueWithObjectMetadata>(_ keys: Keys) -> [ValueWithObjectMetadata] where
        Keys: Sequence,
        Keys.Iterator.Element == String,
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            return readAtIndexes(ValueWithObjectMetadata.indexesWithKeys(keys))
    }

    /**
    Reads all the items in the database.

    - returns: an array of `ItemType`
    */
    public func readAll<
        ValueWithObjectMetadata>() -> [ValueWithObjectMetadata] where
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            return readByKeys(keysInCollection(ValueWithObjectMetadata.collection))
    }
}

extension ConnectionType {

    /**
    Reads the item at a given index.

    - parameter index: a YapDB.Index
    - returns: an optional `ItemType`
    */
    public func readAtIndex<
        ValueWithObjectMetadata>(_ index: YapDB.Index) -> ValueWithObjectMetadata? where
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            return read { $0.readAtIndex(index) }
    }

    /**
    Reads the items at the indexes.

    - parameter indexes: a SequenceType of YapDB.Index values
    - returns: an array of `ItemType`
    */
    public func readAtIndexes<
        Indexes, ValueWithObjectMetadata>(_ indexes: Indexes) -> [ValueWithObjectMetadata] where
        Indexes: Sequence,
        Indexes.Iterator.Element == YapDB.Index,
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            return read { $0.readAtIndexes(indexes) }
    }

    /**
    Reads the item at the key.

    - parameter key: a String
    - returns: an optional `ItemType`
    */
    public func readByKey<
        ValueWithObjectMetadata>(_ key: String) -> ValueWithObjectMetadata? where
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            return readAtIndex(ValueWithObjectMetadata.indexWithKey(key))
    }

    /**
    Reads the items by the keys.

    - parameter keys: a SequenceType of String values
    - returns: an array of `ItemType`
    */
    public func readByKeys<
        Keys, ValueWithObjectMetadata>(_ keys: Keys) -> [ValueWithObjectMetadata] where
        Keys: Sequence,
        Keys.Iterator.Element == String,
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            return readAtIndexes(ValueWithObjectMetadata.indexesWithKeys(keys))
    }

    /**
    Reads all the items in the database.

    - returns: an array of `ItemType`
    */
    public func readAll<
        ValueWithObjectMetadata>() -> [ValueWithObjectMetadata] where
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            return read { $0.readAll() }
    }
}

// MARK: - Writing

extension WriteTransactionType {

    /**
    Write the item to the database using the transaction.

    - parameter item: the item to store.
    */
    public func write<
        ValueWithObjectMetadata>(_ item: ValueWithObjectMetadata) -> ValueWithObjectMetadata where
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            writeAtIndex(item.index, object: item.encoded, metadata: item.metadata)
            return item
    }

    /**
    Write the items to the database using the transaction.

    - parameter items: a SequenceType of items to store.
    */
    public func write<
        Items, ValueWithObjectMetadata>(_ items: Items) -> [ValueWithObjectMetadata] where
        Items: Sequence,
        Items.Iterator.Element == ValueWithObjectMetadata,
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            return items.map(write)
    }
}

extension ConnectionType {

    /**
    Write the item to the database synchronously using the connection in a new transaction.

    - parameter item: the item to store.
    */
    public func write<
        ValueWithObjectMetadata>(_ item: ValueWithObjectMetadata) -> ValueWithObjectMetadata where
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            return write { $0.write(item) }
    }

    /**
    Write the items to the database synchronously using the connection in a new transaction.

    - parameter items: a SequenceType of items to store.
    */
    public func write<
        Items, ValueWithObjectMetadata>(_ items: Items) -> [ValueWithObjectMetadata] where
        Items: Sequence,
        Items.Iterator.Element == ValueWithObjectMetadata,
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            return write { $0.write(items) }
    }

    /**
    Write the item to the database asynchronously using the connection in a new transaction.

    - parameter item: the item to store.
    - parameter queue: the dispatch_queue_t to run the completion block on.
    - parameter completion: a dispatch_block_t for completion.
    */
    public func asyncWrite<
        ValueWithObjectMetadata>(_ item: ValueWithObjectMetadata, queue: DispatchQueue = DispatchQueue.main, completion: ((ValueWithObjectMetadata) -> Void)? = nil) where
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            asyncWrite({ $0.write(item) }, queue: queue, completion: completion)
    }

    /**
    Write the items to the database asynchronously using the connection in a new transaction.

    - parameter items: a SequenceType of items to store.
    - parameter queue: the dispatch_queue_t to run the completion block on.
    - parameter completion: a dispatch_block_t for completion.
    */
    public func asyncWrite<
        Items, ValueWithObjectMetadata>(_ items: Items, queue: DispatchQueue = DispatchQueue.main, completion: (([ValueWithObjectMetadata]) -> Void)? = nil) where
        Items: Sequence,
        Items.Iterator.Element == ValueWithObjectMetadata,
        ValueWithObjectMetadata: Persistable,
        ValueWithObjectMetadata: ValueCoding,
        ValueWithObjectMetadata.Coder: NSCoding,
        ValueWithObjectMetadata.Coder.Value == ValueWithObjectMetadata,
        ValueWithObjectMetadata.MetadataType: NSCoding {
            asyncWrite({ $0.write(items) }, queue: queue, completion: completion)
    }
}

