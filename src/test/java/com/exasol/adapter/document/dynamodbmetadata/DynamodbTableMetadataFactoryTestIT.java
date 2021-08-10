package com.exasol.adapter.document.dynamodbmetadata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.adapter.document.DynamodbTestDbBuilder;
import com.exasol.adapter.document.TestcontainerDynamodbTestDbBuilder;
import com.exasol.dynamodb.DynamodbContainer;

import software.amazon.awssdk.services.dynamodb.model.*;

@Tag("integration")
@Tag("quick")
@Testcontainers
class DynamodbTableMetadataFactoryTestIT {
    private static final String TABLE_NAME = "test";
    @Container
    private static final DynamodbContainer DYNAMODB = new DynamodbContainer();
    private static DynamodbTableMetadataFactory tableMetadataFactory;
    private static final String PARTITION_KEY = "partition_key";
    private static final String SORT_KEY = "sort_key";
    private static DynamodbTestDbBuilder dynamodbTestDbBuilder;

    @BeforeAll
    static void beforeAll() throws DynamodbTestDbBuilder.NoNetworkFoundException, IOException, URISyntaxException {
        dynamodbTestDbBuilder = new TestcontainerDynamodbTestDbBuilder(DYNAMODB);
        tableMetadataFactory = new BaseDynamodbTableMetadataFactory(
                dynamodbTestDbBuilder.getDynamodbLowLevelConnection());
    }

    @AfterEach
    void afterEach() {
        dynamodbTestDbBuilder.deleteCreatedTables();
    }

    @Test
    void testSimplePrimaryKey() {
        final String keyName = "primary_key";
        dynamodbTestDbBuilder.createTable(TABLE_NAME, keyName);
        final DynamodbTableMetadata dynamodbTableMetadata = tableMetadataFactory.buildMetadataForTable(TABLE_NAME);
        assertAll(//
                () -> assertThat(dynamodbTableMetadata.getPrimaryIndex().getPartitionKey(), equalTo(keyName)),
                () -> assertThat(dynamodbTableMetadata.getPrimaryIndex().hasSortKey(), equalTo(false)), //
                () -> assertThat(dynamodbTableMetadata.getPrimaryIndex(), instanceOf(DynamodbPrimaryIndex.class))//
        );
    }

    @Test
    void testSearchKey() {
        final CreateTableRequest.Builder requestBuilder = getBasicCreateTableRequest(Collections.emptyList());
        dynamodbTestDbBuilder.createTable(requestBuilder.build());
        final DynamodbTableMetadata dynamodbTableMetadata = tableMetadataFactory.buildMetadataForTable(TABLE_NAME);
        assertAll(//
                () -> assertThat(dynamodbTableMetadata.getPrimaryIndex().getPartitionKey(), equalTo(PARTITION_KEY)),
                () -> assertThat(dynamodbTableMetadata.getPrimaryIndex().getSortKey(), equalTo(SORT_KEY)), //
                () -> assertThat(dynamodbTableMetadata.getPrimaryIndex(), instanceOf(DynamodbPrimaryIndex.class))//
        );
    }

    private CreateTableRequest.Builder getBasicCreateTableRequest(
            final List<AttributeDefinition> additionalAttributeDefinitions) {
        final List<AttributeDefinition> attributeDefinitions = new ArrayList<>(List.of(
                AttributeDefinition.builder().attributeName(PARTITION_KEY).attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName(SORT_KEY).attributeType(ScalarAttributeType.S).build()));
        attributeDefinitions.addAll(additionalAttributeDefinitions);
        final CreateTableRequest.Builder requestBuilder = CreateTableRequest.builder();
        requestBuilder.tableName(TABLE_NAME);
        requestBuilder.keySchema(
                List.of(KeySchemaElement.builder().attributeName(PARTITION_KEY).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(SORT_KEY).keyType(KeyType.RANGE).build()));
        requestBuilder.attributeDefinitions(attributeDefinitions);
        requestBuilder.provisionedThroughput(
                ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build());
        return requestBuilder;
    }

    @Test
    void testLocalIndex() {
        final String indexKey = "index_key";
        final String indexName = "indexName";
        final CreateTableRequest.Builder requestBuilder = getBasicCreateTableRequest(List.of(
                AttributeDefinition.builder().attributeName(indexKey).attributeType(ScalarAttributeType.S).build()));
        requestBuilder.localSecondaryIndexes(LocalSecondaryIndex.builder()
                .keySchema(KeySchemaElement.builder().attributeName(PARTITION_KEY).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(indexKey).keyType(KeyType.RANGE).build())
                .indexName(indexName).projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                .build());
        dynamodbTestDbBuilder.createTable(requestBuilder.build());
        final DynamodbTableMetadata dynamodbTableMetadata = tableMetadataFactory.buildMetadataForTable(TABLE_NAME);
        final DynamodbIndex index = dynamodbTableMetadata.getLocalIndexes().get(0);
        assertAll(//
                () -> assertThat(index.getPartitionKey(), equalTo(PARTITION_KEY)), //
                () -> assertThat(index.getSortKey(), equalTo(indexKey)), //
                () -> assertThat(((DynamodbSecondaryIndex) index).getIndexName(), equalTo(indexName)));
    }

    @Test
    void testGlobalIndex() {
        final String indexKey1 = "index_key1";
        final String indexKey2 = "index_key2";
        final String indexName = "indexName";
        final CreateTableRequest.Builder requestBuilder = getBasicCreateTableRequest(List.of(
                AttributeDefinition.builder().attributeName(indexKey1).attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName(indexKey2).attributeType(ScalarAttributeType.S).build()));
        requestBuilder.globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                .keySchema(KeySchemaElement.builder().attributeName(indexKey1).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(indexKey2).keyType(KeyType.RANGE).build())
                .indexName(indexName)
                .provisionedThroughput(builder -> builder.readCapacityUnits(1L).writeCapacityUnits(1L).build())
                .projection(builder -> builder.projectionType(ProjectionType.KEYS_ONLY)).build());
        dynamodbTestDbBuilder.createTable(requestBuilder.build());
        final DynamodbTableMetadata dynamodbTableMetadata = tableMetadataFactory.buildMetadataForTable(TABLE_NAME);
        final DynamodbSecondaryIndex index = dynamodbTableMetadata.getGlobalIndexes().get(0);
        assertAll(//
                () -> assertThat(index.getPartitionKey(), equalTo(indexKey1)), //
                () -> assertThat(index.getSortKey(), equalTo(indexKey2)), //
                () -> assertThat(index.getIndexName(), equalTo(indexName)));
    }
}