package com.exasol.adapter.dynamodb.dynamodbmetadata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;

import com.amazonaws.services.dynamodbv2.model.*;
import com.exasol.adapter.dynamodb.DynamodbTestInterface;
import com.exasol.adapter.dynamodb.IntegrationTestSetup;

@Tag("integration")
@Tag("quick")
class DynamodbTableMetadataFactoryTestIT {
    private static final String TABLE_NAME = "test";
    private static DynamodbTestInterface dynamodbTestInterface;
    private static DynamodbTableMetadataFactory tableMetadataFactory;

    @BeforeAll
    static void beforeAll() throws DynamodbTestInterface.NoNetworkFoundException, IOException {
        dynamodbTestInterface = new IntegrationTestSetup().getDynamodbTestInterface();
        tableMetadataFactory = new BaseDynamodbTableMetadataFactory(
                dynamodbTestInterface.getDynamodbLowLevelConnection());
    }

    @AfterAll
    static void afterAll() {
        dynamodbTestInterface.teardown();
    }

    @AfterEach
    void afterEach() {
        dynamodbTestInterface.deleteCreatedTables();
    }

    @Test
    void testSimplePrimaryKey() {
        final String keyName = "primary_key";
        dynamodbTestInterface.createTable(TABLE_NAME, keyName);
        final DynamodbTableMetadata dynamodbTableMetadata = tableMetadataFactory.buildMetadataForTable(TABLE_NAME);
        assertAll(//
                () -> assertThat(dynamodbTableMetadata.getPrimaryIndex().getPartitionKey(), equalTo(keyName)),
                () -> assertThat(dynamodbTableMetadata.getPrimaryIndex().hasSortKey(), equalTo(false)), //
                () -> assertThat(dynamodbTableMetadata.getPrimaryIndex(), instanceOf(DynamodbPrimaryIndex.class))//
        );
    }

    @Test
    void testSearchKey() {
        final String partitionKey = "partition_key";
        final String sortKey = "sort_key";
        final CreateTableRequest request = new CreateTableRequest().withTableName(TABLE_NAME)
                .withKeySchema(new KeySchemaElement(partitionKey, KeyType.HASH),
                        new KeySchemaElement(sortKey, KeyType.RANGE))
                .withAttributeDefinitions(new AttributeDefinition(partitionKey, ScalarAttributeType.S),
                        new AttributeDefinition(sortKey, ScalarAttributeType.S))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
        dynamodbTestInterface.createTable(request);
        final DynamodbTableMetadata dynamodbTableMetadata = tableMetadataFactory.buildMetadataForTable(TABLE_NAME);
        assertAll(//
                () -> assertThat(dynamodbTableMetadata.getPrimaryIndex().getPartitionKey(), equalTo(partitionKey)),
                () -> assertThat(dynamodbTableMetadata.getPrimaryIndex().getSortKey(), equalTo(sortKey)), //
                () -> assertThat(dynamodbTableMetadata.getPrimaryIndex(), instanceOf(DynamodbPrimaryIndex.class))//
        );
    }

    @Test
    void testLocalIndex() {
        final String partitionKey = "partition_key";
        final String sortKey = "sort_key";
        final String indexKey = "index_key";
        final String indexName = "indexName";
        final CreateTableRequest request = new CreateTableRequest().withTableName(TABLE_NAME)
                .withKeySchema(new KeySchemaElement(partitionKey, KeyType.HASH),
                        new KeySchemaElement(sortKey, KeyType.RANGE))
                .withAttributeDefinitions(new AttributeDefinition(partitionKey, ScalarAttributeType.S),
                        new AttributeDefinition(sortKey, ScalarAttributeType.S),
                        new AttributeDefinition(indexKey, ScalarAttributeType.S))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))//
                .withLocalSecondaryIndexes(new LocalSecondaryIndex()
                        .withKeySchema(new KeySchemaElement(partitionKey, KeyType.HASH),
                                new KeySchemaElement(indexKey, KeyType.RANGE))
                        .withIndexName(indexName).withProjection(new Projection().withProjectionType("KEYS_ONLY")));
        dynamodbTestInterface.createTable(request);
        final DynamodbTableMetadata dynamodbTableMetadata = tableMetadataFactory.buildMetadataForTable(TABLE_NAME);
        final DynamodbIndex index = dynamodbTableMetadata.getLocalIndexes().get(0);
        assertAll(//
                () -> assertThat(index.getPartitionKey(), equalTo(partitionKey)), //
                () -> assertThat(index.getSortKey(), equalTo(indexKey)), //
                () -> assertThat(((DynamodbSecondaryIndex) index).getIndexName(), equalTo(indexName)));
    }

    @Test
    void testGlobalIndex() {
        final String partitionKey = "partition_key";
        final String sortKey = "sort_key";
        final String indexKey1 = "index_key1";
        final String indexKey2 = "index_key2";
        final String indexName = "indexName";
        final CreateTableRequest request = new CreateTableRequest().withTableName(TABLE_NAME)
                .withKeySchema(new KeySchemaElement(partitionKey, KeyType.HASH),
                        new KeySchemaElement(sortKey, KeyType.RANGE))
                .withAttributeDefinitions(new AttributeDefinition(partitionKey, ScalarAttributeType.S),
                        new AttributeDefinition(sortKey, ScalarAttributeType.S),
                        new AttributeDefinition(indexKey1, ScalarAttributeType.S),
                        new AttributeDefinition(indexKey2, ScalarAttributeType.S))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))//
                .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
                        .withKeySchema(new KeySchemaElement(indexKey1, KeyType.HASH),
                                new KeySchemaElement(indexKey2, KeyType.RANGE))
                        .withIndexName(indexName).withProjection(new Projection().withProjectionType("KEYS_ONLY"))
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)));
        dynamodbTestInterface.createTable(request);
        final DynamodbTableMetadata dynamodbTableMetadata = tableMetadataFactory.buildMetadataForTable(TABLE_NAME);
        final DynamodbSecondaryIndex index = dynamodbTableMetadata.getGlobalIndexes().get(0);
        assertAll(//
                () -> assertThat(index.getPartitionKey(), equalTo(indexKey1)), //
                () -> assertThat(index.getSortKey(), equalTo(indexKey2)), //
                () -> assertThat(index.getIndexName(), equalTo(indexName)));
    }
}