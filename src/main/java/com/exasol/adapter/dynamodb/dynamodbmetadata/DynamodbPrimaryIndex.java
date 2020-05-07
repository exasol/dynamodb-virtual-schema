package com.exasol.adapter.dynamodb.dynamodbmetadata;

import java.util.Optional;

/**
 * This class represents the DynamoDB index that is defined by the primary key of a table.
 */
public class DynamodbPrimaryIndex extends AbstractDynamodbIndex {
    private static final long serialVersionUID = 1990746116313161380L;

    /**
     * Creates an instance of {@link DynamodbPrimaryIndex}.
     *
     * @param partitionKey partition key of this key
     * @param sortKey      sort key of this key
     */
    public DynamodbPrimaryIndex(final String partitionKey, final Optional<String> sortKey) {
        super(partitionKey, sortKey);
    }
}
