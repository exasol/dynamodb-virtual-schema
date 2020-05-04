package com.exasol.adapter.dynamodb.dynamodbmetadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class describes the index structures of a DynamoDB table.
 */
public class DynamodbTableMetadata implements Serializable {
    private static final long serialVersionUID = 7898591572006230212L;
    private final DynamodbIndex primaryIndex;
    private final List<DynamodbIndex> localIndexes;
    private final List<DynamodbIndex> globalIndexes;

    /**
     * Creates an instance of {@link DynamodbTableMetadata}.
     * 
     * @param primaryIndex  the index defined by the primary key
     * @param localIndexes  list of the local secondary indexes
     * @param globalIndexes list of the global secondary indexes
     */
    public DynamodbTableMetadata(final DynamodbIndex primaryIndex, final List<DynamodbIndex> localIndexes,
            final List<DynamodbIndex> globalIndexes) {
        this.primaryIndex = primaryIndex;
        this.localIndexes = localIndexes;
        this.globalIndexes = globalIndexes;
    }

    /**
     * Gives the index defined by the primary key of this DynamoDB table.
     * 
     * @return primary key description.
     */
    public DynamodbIndex getPrimaryIndex() {
        return this.primaryIndex;
    }

    /**
     * Gives the local secondary indexes of this table.
     * 
     * @return list of index descriptions
     */
    public List<DynamodbIndex> getLocalIndexes() {
        return this.localIndexes;
    }

    /**
     * Gives the global secondary indexes of this table.
     *
     * @return list of index descriptions
     */
    public List<DynamodbIndex> getGlobalIndexes() {
        return this.globalIndexes;
    }

    /**
     * Gives the local and global secondary secondary indexes of this table.
     *
     * @return list of index descriptions
     */
    public List<DynamodbIndex> getSecondaryIndexes() {
        final ArrayList<DynamodbIndex> union = new ArrayList<>(this.localIndexes.size() + this.globalIndexes.size());
        union.addAll(this.localIndexes);
        union.addAll(this.globalIndexes);
        return union;
    }

    /**
     * Gives a list containing the primary index and all local and global indexes.
     * 
     * @return list of all keys
     */
    public List<DynamodbIndex> getAllIndexes() {
        final ArrayList<DynamodbIndex> union = new ArrayList<>(this.localIndexes.size() + this.globalIndexes.size());
        union.addAll(this.localIndexes);
        union.addAll(this.globalIndexes);
        union.add(getPrimaryIndex());
        return union;
    }
}
