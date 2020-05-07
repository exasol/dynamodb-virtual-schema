package com.exasol.adapter.dynamodb.dynamodbmetadata;

import java.io.Serializable;
import java.util.Optional;

/**
 * This class is the abstract base for a DynamoDB indexes.
 */
public abstract class AbstractDynamodbIndex implements Serializable {
    private static final long serialVersionUID = 4048033058983909214L;
    private final String partitionKey;
    private final String sortKey;

    /**
     * Creates an instance of {@link AbstractDynamodbIndex}.
     * 
     * @param partitionKey partition key of this index
     * @param sortKey      sort key of this index
     */
    public AbstractDynamodbIndex(final String partitionKey, final Optional<String> sortKey) {
        this.partitionKey = partitionKey;
        this.sortKey = sortKey.orElse(null);
    }

    /**
     * Gives the partition key of this index.
     * 
     * @return partition key
     */
    public String getPartitionKey() {
        return this.partitionKey;
    }

    /**
     * Gives the sort key of this index.
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
