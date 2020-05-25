package com.exasol.adapter.dynamodb.dynamodbmetadata;

import java.io.Serializable;

/**
 * This interface gives access to the keys of a DynamoDB index.
 */
public interface DynamodbIndex extends Serializable {

    /**
     * Get the partition key of this index.
     *
     * @return partition key
     */
    public String getPartitionKey();

    /**
     * Get the sort key of this index.
     *
     * @return sort key. {@code null} if no sort key is present.
     */
    public String getSortKey();

    /**
     * Tests if sort key is present.
     *
     * @return {@code true} if sort key is present.
     */
    public boolean hasSortKey();
}
