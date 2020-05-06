package com.exasol.adapter.dynamodb.dynamodbmetadata;

import java.io.Serializable;
import java.util.Optional;

/**
 * This class represents a DynamoDB index. This can either be the primary index of a table or a secondary index.
 */
public abstract class AbstractDynamodbIndex implements Serializable {
    private static final long serialVersionUID = 4048033058983909214L;
    private final String partitionKey;
    private final String sortKey;

    /**
     * Creates an instance of {@link AbstractDynamodbIndex}.
     * 
     * @param partitionKey partition key of this key
     * @param sortKey      sort key of this key
     */
    public AbstractDynamodbIndex(final String partitionKey, final Optional<String> sortKey) {
        this.partitionKey = partitionKey;
        this.sortKey = sortKey.orElse(null);
    }

    /**
     * Gives the partition key of this key.
     * 
     * @return partition key
     */
    public String getPartitionKey() {
        return this.partitionKey;
    }

    /**
     * Gives the sort key of this key.
     * 
     * @return sort key. {@code null} if no sort key is present.
     */
    public String getSortKey() {
        return this.sortKey;
    }

    /**
     * Tests if sort key is present.
     * 
     * @return {@code true} if sort key is present.
     */
    public boolean hasSortKey() {
        return this.sortKey != null;
    }
}
