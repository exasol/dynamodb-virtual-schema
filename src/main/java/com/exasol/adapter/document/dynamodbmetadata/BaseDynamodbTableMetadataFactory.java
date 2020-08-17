package com.exasol.adapter.document.dynamodbmetadata;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * This class builds {@link DynamodbTableMetadata} by fetching the required information using a {@code describeTable}
 * call to DynamoDB.
 */
public class BaseDynamodbTableMetadataFactory implements DynamodbTableMetadataFactory {
    private final DynamoDbClient connection;

    /**
     * Create an instance of {@link BaseDynamodbTableMetadataFactory}.
     *
     * @param connection DynamoDB connection used for {@code describeTable} call
     */
    public BaseDynamodbTableMetadataFactory(final DynamoDbClient connection) {
        this.connection = connection;
    }

    @Override
    public DynamodbTableMetadata buildMetadataForTable(final String tableName) {
        final TableDescription tableDescription = this.connection
                .describeTable(DescribeTableRequest.builder().tableName(tableName).build()).table();
        final List<KeySchemaElement> keySchema = tableDescription.keySchema();
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
            if (keySchemaElement.keyType().equals(KeyType.HASH)) {
                return keySchemaElement.attributeName();
            }
        }
        throw new IllegalStateException("Could not find partition key. "
                + "This should not happen because each Dynamodb table must define a partition key.");
    }

    private Optional<String> extractSortKey(final List<KeySchemaElement> keySchema) {
        for (final KeySchemaElement keySchemaElement : keySchema) {
            if (keySchemaElement.keyType().equals(KeyType.RANGE)) {
                return Optional.of(keySchemaElement.attributeName());
            }
        }
        return Optional.empty();
    }

    private List<DynamodbSecondaryIndex> extractLocalSecondaryIndex(final TableDescription tableDescription) {
        final List<LocalSecondaryIndexDescription> localSecondaryIndexes = tableDescription.localSecondaryIndexes();
        if (localSecondaryIndexes != null) {
            return localSecondaryIndexes.stream()
                    .map(index -> extractSecondaryIndex(index.keySchema(), index.indexName()))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private List<DynamodbSecondaryIndex> extractGlobalSecondaryIndex(final TableDescription tableDescription) {
        final List<GlobalSecondaryIndexDescription> globalSecondaryIndexes = tableDescription
                .globalSecondaryIndexes();
        if (globalSecondaryIndexes != null) {
            return globalSecondaryIndexes.stream()
                    .map(index -> extractSecondaryIndex(index.keySchema(), index.indexName()))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }
}
