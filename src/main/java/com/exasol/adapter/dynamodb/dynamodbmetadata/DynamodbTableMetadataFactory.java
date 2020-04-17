package com.exasol.adapter.dynamodb.dynamodbmetadata;

import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

public class DynamodbTableMetadataFactory {
    public DynamodbTableMetadata forDynamodbTable(final AmazonDynamoDB connection, final String tableName) {
        final TableDescription tableDescription = connection.describeTable(tableName).getTable();
        final DynamodbKey primaryKey = extractKey(tableDescription.getKeySchema());
        final List<DynamodbKey> localIndexes = extractLocalSecondaryIndex(tableDescription);
        final List<DynamodbKey> globalIndexes = extractGlobalSecondaryIndex(tableDescription);
        return new DynamodbTableMetadata(primaryKey, localIndexes, globalIndexes);
    }

    private DynamodbKey extractKey(final List<KeySchemaElement> keySchema) {
        final DocumentPathExpression partitionKey = extractPartitionKey(keySchema);
        final DocumentPathExpression sortKey = extractSortKey(keySchema);
        return new DynamodbKey(partitionKey, sortKey);
    }

    private DocumentPathExpression extractPartitionKey(final List<KeySchemaElement> keySchema) {
        for (final KeySchemaElement keySchemaElement : keySchema) {
            if (keySchemaElement.getKeyType().equals("HASH")) {
                final String keyName = keySchemaElement.getAttributeName();
                return new DocumentPathExpression.Builder().addObjectLookup(keyName).build();
            }
        }
        throw new IllegalStateException("Could not find partition key. "
                + "That's strange because each Dynamodb table must define a partition key.");
    }

    private DocumentPathExpression extractSortKey(final List<KeySchemaElement> keySchema) {
        for (final KeySchemaElement keySchemaElement : keySchema) {
            if (keySchemaElement.getKeyType().equals("RANGE")) {
                final String keyName = keySchemaElement.getAttributeName();
                return new DocumentPathExpression.Builder().addObjectLookup(keyName).build();
            }
        }
        return null;
    }

    private List<DynamodbKey> extractLocalSecondaryIndex(final TableDescription tableDescription) {
        if(tableDescription.getLocalSecondaryIndexes() != null) {
            return tableDescription.getLocalSecondaryIndexes().stream().map(index ->
                    extractKey(index.getKeySchema()))
                    .collect(Collectors.toList());
        }
        else{
            return List.of();
        }
    }

    private List<DynamodbKey> extractGlobalSecondaryIndex(final TableDescription tableDescription) {
        if(tableDescription.getGlobalSecondaryIndexes() != null) {
            return tableDescription.getGlobalSecondaryIndexes().stream().map(index ->
                    extractKey(index.getKeySchema()))
                    .collect(Collectors.toList());
        }
        else{
            return List.of();
        }
    }
}
