package com.exasol.adapter.dynamodb.dynamodbmetadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class describes the index structures of a DynamoDB table.
 */
public class DynamodbTableMetadata implements Serializable {
    private static final long serialVersionUID = 7898591572006230212L;
    private final DynamodbPrimaryIndex primaryIndex;
    private final List<DynamodbSecondaryIndex> localIndexes;
    private final List<DynamodbSecondaryIndex> globalIndexes;

    /**
     * Create an instance of {@link DynamodbTableMetadata}.
     * 
     * @param primaryIndex  the index defined by the primary key
     * @param localIndexes  list of the local secondary indexes
     * @param globalIndexes list of the global secondary indexes
     */
    public DynamodbTableMetadata(final DynamodbPrimaryIndex primaryIndex,
            final List<DynamodbSecondaryIndex> localIndexes, final List<DynamodbSecondaryIndex> globalIndexes) {
        this.primaryIndex = primaryIndex;
        this.localIndexes = localIndexes;
        this.globalIndexes = globalIndexes;
    }

    /**
     * Get the index defined by the primary key of this DynamoDB table.
     * 
     * @return primary index description.
     */
    public DynamodbPrimaryIndex getPrimaryIndex() {
        return this.primaryIndex;
    }

    /**
     * Get the local secondary indexes of this table.
     * 
     * @return list of index descriptions
     */
    public List<DynamodbSecondaryIndex> getLocalIndexes() {
        return this.localIndexes;
    }

    /**
     * Get the global secondary indexes of this table.
     *
     * @return list of index descriptions
     */
    public List<DynamodbSecondaryIndex> getGlobalIndexes() {
        return this.globalIndexes;
    }

    /**
     * Get the local and global secondary secondary indexes of this table.
     *
     * @return list of index descriptions
     */
    public List<DynamodbSecondaryIndex> getSecondaryIndexes() {
        final ArrayList<DynamodbSecondaryIndex> union = new ArrayList<>(
                this.localIndexes.size() + this.globalIndexes.size());
        union.addAll(this.localIndexes);
        union.addAll(this.globalIndexes);
        return union;
    }

    /**
     * Get a list containing the primary index and all local and global indexes.
     * 
     * @return list of all indexes
     */
    public List<DynamodbIndex> getAllIndexes() {
        final ArrayList<DynamodbIndex> union = new ArrayList<>(this.localIndexes.size() + this.globalIndexes.size());
        union.addAll(this.localIndexes);
        union.addAll(this.globalIndexes);
        union.add(getPrimaryIndex());
        return union;
    }
}
