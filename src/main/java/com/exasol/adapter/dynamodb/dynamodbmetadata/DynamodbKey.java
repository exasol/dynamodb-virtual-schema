package com.exasol.adapter.dynamodb.dynamodbmetadata;

import java.io.Serializable;
import java.util.Optional;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class represents a DynamoDB key. It can either be a primary key or a secondary index.
 */
public class DynamodbKey implements Serializable {
    private static final long serialVersionUID = 4048033058983909214L;
    private final DocumentPathExpression partitionKey;
    private final DocumentPathExpression sortKey;

    /**
     * Creates an instance of {@link DynamodbKey}.
     * 
     * @param partitionKey partition key of this key
     * @param sortKey      sort key of this key
     */
    public DynamodbKey(final DocumentPathExpression partitionKey, final Optional<DocumentPathExpression> sortKey) {
        this.partitionKey = partitionKey;
        this.sortKey = sortKey.orElse(null);
    }

    /**
     * Gives the partition key of this key.
     * 
     * @return partition key
     */
    public DocumentPathExpression getPartitionKey() {
        return this.partitionKey;
    }

    /**
     * Gives the sort key of this key.
     * 
     * @return sort key. {@code null} if no sort key is present.
     */
    public DocumentPathExpression getSortKey() {
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
