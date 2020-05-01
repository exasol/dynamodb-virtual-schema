package com.exasol.adapter.dynamodb.dynamodbmetadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class describes the primary key and indexes of a DynamoDB table.
 */
public class DynamodbTableMetadata implements Serializable {
    private static final long serialVersionUID = 7898591572006230212L;
    private final DynamodbKey primaryKey;
    private final List<DynamodbKey> localIndexes;
    private final List<DynamodbKey> globalIndexes;

    /**
     * Creates an instance of {@link DynamodbTableMetadata}.
     * 
     * @param primaryKey    the primary key of the table
     * @param localIndexes  list of the local secondary indexes
     * @param globalIndexes list of the global secondary indexes
     */
    public DynamodbTableMetadata(final DynamodbKey primaryKey, final List<DynamodbKey> localIndexes,
            final List<DynamodbKey> globalIndexes) {
        this.primaryKey = primaryKey;
        this.localIndexes = localIndexes;
        this.globalIndexes = globalIndexes;
    }

    /**
     * Gives the primary key of this DynamoDB table.
     * 
     * @return primary key description.
     */
    public DynamodbKey getPrimaryKey() {
        return this.primaryKey;
    }

    /**
     * Gives the local secondary indexes of this table.
     * 
     * @return list of index descriptions
     */
    public List<DynamodbKey> getLocalIndexes() {
        return this.localIndexes;
    }

    /**
     * Gives the global secondary indexes of this table.
     *
     * @return list of index descriptions
     */
    public List<DynamodbKey> getGlobalIndexes() {
        return this.globalIndexes;
    }

    /**
     * Gives the local and global secondary indexes of this table.
     *
     * @return list of index descriptions
     */
    public List<DynamodbKey> getAllIndexes() {
        final ArrayList<DynamodbKey> union = new ArrayList<>(this.localIndexes.size() + this.globalIndexes.size());
        union.addAll(this.localIndexes);
        union.addAll(this.globalIndexes);
        return union;
    }

    /**
     * Gives a list containing the primary key and all local and global indexes.
     * 
     * @return list of all keys
     */
    public List<DynamodbKey> getAllKeys() {
        final ArrayList<DynamodbKey> union = new ArrayList<>(this.localIndexes.size() + this.globalIndexes.size());
        union.addAll(this.localIndexes);
        union.addAll(this.globalIndexes);
        union.add(getPrimaryKey());
        return union;
    }
}
