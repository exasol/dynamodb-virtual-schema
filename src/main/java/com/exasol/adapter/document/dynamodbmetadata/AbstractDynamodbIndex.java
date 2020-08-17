package com.exasol.adapter.document.dynamodbmetadata;

import java.util.Optional;

/**
 * This class is the abstract base for a DynamoDB indexes.
 */
abstract class AbstractDynamodbIndex implements DynamodbIndex {

    private final String partitionKey;
    private final String sortKey;

    /**
     * Create an instance of {@link DynamodbIndex}.
     *
     * @param partitionKey partition key of this index
     * @param sortKey      sort key of this index
     */
    public AbstractDynamodbIndex(final String partitionKey, final Optional<String> sortKey) {
        this.partitionKey = partitionKey;
        this.sortKey = sortKey.orElse(null);
    }

    @Override
    public String getPartitionKey() {
        return this.partitionKey;
    }

    @Override
    public String getSortKey() {
        return this.sortKey;
    }

    @Override
    public boolean hasSortKey() {
        return this.sortKey != null;
    }
}
