package com.exasol.adapter.dynamodb.dynamodbmetadata;

import java.util.Optional;

/**
 * This class represents a secondary DynamoDB index.
 */
public class DynamodbSecondaryIndex extends AbstractDynamodbIndex {
    private static final long serialVersionUID = 3832156979432859628L;
    private final String indexName;

    /**
     * Create an instance of {@link DynamodbSecondaryIndex}.
     * 
     * @param partitionKey partition key of this key
     * @param sortKey      sort key of this key
     * @param indexName    DynamoDB index name
     */
    public DynamodbSecondaryIndex(final String partitionKey, final Optional<String> sortKey, final String indexName) {
        super(partitionKey, sortKey);
        this.indexName = indexName;
    }

    /**
     * Get the DynamoDB name for the index.
     * 
     * @return name of the index
     */
    public String getIndexName() {
        return this.indexName;
    }
}
