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

/**
 * This class builds {@link DynamodbTableMetadata} by fetching the required information using a {@code describeTable}
 * call to DynamoDB.
 */
public class DynamodbTableMetadataFactory {
    private final AmazonDynamoDB connection;

    /**
     * Creates an instance of {@link DynamodbTableMetadataFactory}.
     *
     * @param connection DynamoDB connection used for {@code describeTable} call
     */
    public DynamodbTableMetadataFactory(final AmazonDynamoDB connection) {
        this.connection = connection;
    }

    /**
     * Builds {@link DynamodbTableMetadata} for a DynamoDB table.
     *
     * @param tableName DynamoDB table name
     * @return {@link DynamodbTableMetadata} describing the keys and indexes of the table
     */
    public DynamodbTableMetadata buildMetadataForTable(final String tableName) {
        final TableDescription tableDescription = this.connection.describeTable(tableName).getTable();
        final List<KeySchemaElement> keySchema = tableDescription.getKeySchema();
        final DynamodbPrimaryIndex primaryKey = new DynamodbPrimaryIndex(extractPartitionKey(keySchema),
                extractSortKey(keySchema));
        final List<DynamodbSecondaryIndex> localIndexes = extractLocalSecondaryIndex(tableDescription);
        final List<DynamodbSecondaryIndex> globalIndexes = extractGlobalSecondaryIndex(tableDescription);
        return new DynamodbTableMetadata(primaryKey, localIndexes, globalIndexes);
    }

    private DynamodbSecondaryIndex extractSecondaryIndex(final List<KeySchemaElement> keySchema,
            final String indexName) {
        return new DynamodbSecondaryIndex(extractPartitionKey(keySchema), extractSortKey(keySchema), indexName);
    }

    private String extractPartitionKey(final List<KeySchemaElement> keySchema) {
        for (final KeySchemaElement keySchemaElement : keySchema) {
            if (keySchemaElement.getKeyType().equals("HASH")) {
                return keySchemaElement.getAttributeName();
            }
        }
        throw new IllegalStateException("Could not find partition key. "
                + "This should not happen because each Dynamodb table must define a partition key.");
    }

    private Optional<String> extractSortKey(final List<KeySchemaElement> keySchema) {
        for (final KeySchemaElement keySchemaElement : keySchema) {
            if (keySchemaElement.getKeyType().equals("RANGE")) {
                return Optional.of(keySchemaElement.getAttributeName());
            }
        }
        return Optional.empty();
    }

    private List<DynamodbSecondaryIndex> extractLocalSecondaryIndex(final TableDescription tableDescription) {
        final List<LocalSecondaryIndexDescription> localSecondaryIndexes = tableDescription.getLocalSecondaryIndexes();
        if (localSecondaryIndexes != null) {
            return localSecondaryIndexes.stream()
                    .map(index -> extractSecondaryIndex(index.getKeySchema(), index.getIndexName()))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private List<DynamodbSecondaryIndex> extractGlobalSecondaryIndex(final TableDescription tableDescription) {
        final List<GlobalSecondaryIndexDescription> globalSecondaryIndexes = tableDescription
                .getGlobalSecondaryIndexes();
        if (globalSecondaryIndexes != null) {
            return globalSecondaryIndexes.stream()
                    .map(index -> extractSecondaryIndex(index.getKeySchema(), index.getIndexName()))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }
}
