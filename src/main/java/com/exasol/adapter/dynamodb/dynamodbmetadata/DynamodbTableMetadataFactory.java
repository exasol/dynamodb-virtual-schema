package com.exasol.adapter.dynamodb.dynamodbmetadata;

/**
 * This interface builds {@link DynamodbTableMetadata} by fetching the required information using a
 * {@code describeTable} call to DynamoDB.
 */
public interface DynamodbTableMetadataFactory {
    /**
     * Builds {@link DynamodbTableMetadata} for a DynamoDB table.
     *
     * @param tableName DynamoDB table name
     * @return {@link DynamodbTableMetadata} describing the keys and indexes of the table
     */
    public DynamodbTableMetadata buildMetadataForTable(String tableName);
}
