package com.exasol.adapter.dynamodb.dynamodbmetadata;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class builds {@link DynamodbTableMetadata} by fetching the required information using a {@code describeTable}
 * call to DynamoDB.
 */
public class DynamodbTableMetadataFactory {

    /**
     * Builds {@link DynamodbTableMetadata} for a DynamoDB table.
     * 
     * @param connection DynamoDB connection used for {@code describeTable} call
     * @param tableName  DynamoDB table name
     * @return {@link DynamodbTableMetadata} describing the keys and indexes of the table
     */
    public DynamodbTableMetadata buildMetadataForTable(final AmazonDynamoDB connection, final String tableName) {
        final TableDescription tableDescription = connection.describeTable(tableName).getTable();
        final DynamodbKey primaryKey = extractKey(tableDescription.getKeySchema());
        final List<DynamodbKey> localIndexes = extractLocalSecondaryIndex(tableDescription);
        final List<DynamodbKey> globalIndexes = extractGlobalSecondaryIndex(tableDescription);
        return new DynamodbTableMetadata(primaryKey, localIndexes, globalIndexes);
    }

    private DynamodbKey extractKey(final List<KeySchemaElement> keySchema) {
        final DocumentPathExpression partitionKey = extractPartitionKey(keySchema);
        final Optional<DocumentPathExpression> sortKey = extractSortKey(keySchema);
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
                + "This should not happen because each Dynamodb table must define a partition key.");
    }

    private Optional<DocumentPathExpression> extractSortKey(final List<KeySchemaElement> keySchema) {
        for (final KeySchemaElement keySchemaElement : keySchema) {
            if (keySchemaElement.getKeyType().equals("RANGE")) {
                final String keyName = keySchemaElement.getAttributeName();
                return Optional.of(new DocumentPathExpression.Builder().addObjectLookup(keyName).build());
            }
        }
        return Optional.empty();
    }

    private List<DynamodbKey> extractLocalSecondaryIndex(final TableDescription tableDescription) {
        final List<LocalSecondaryIndexDescription> localSecondaryIndexes = tableDescription.getLocalSecondaryIndexes();
        if (localSecondaryIndexes != null) {
            return localSecondaryIndexes.stream().map(index -> extractKey(index.getKeySchema()))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private List<DynamodbKey> extractGlobalSecondaryIndex(final TableDescription tableDescription) {
        final List<GlobalSecondaryIndexDescription> globalSecondaryIndexes = tableDescription
                .getGlobalSecondaryIndexes();
        if (globalSecondaryIndexes != null) {
            return globalSecondaryIndexes.stream().map(index -> extractKey(index.getKeySchema()))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }
}
