package com.exasol.adapter.dynamodb.dynamodbmetadata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.exasol.adapter.dynamodb.DynamodbTestInterface;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.dynamodb.DynamodbConnectionFactory;

@Tag("integration")
@Tag("quick")
@Testcontainers
class DynamodbTableMetadataFactoryTestIT {
    private static final Network NETWORK = Network.newNetwork();
    @Container
    public static final GenericContainer LOCAL_DYNAMO = new GenericContainer<>("amazon/dynamodb-local")
            .withNetwork(NETWORK).withExposedPorts(8000).withNetworkAliases("dynamo")
            .withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");
    private static DynamodbTestInterface dynamodbTestInterface;

    private static final String TABLE_NAME = "test";

    private AmazonDynamoDB getDynamodbConnection() {
        return new DynamodbConnectionFactory().getLowLevelConnection(dynamodbTestInterface.getDynamoUrl(),
                dynamodbTestInterface.getDynamoUser(), dynamodbTestInterface.getDynamoPass());
    }

    @BeforeAll
    static void beforeAll() throws DynamodbTestInterface.NoNetworkFoundException, SQLException, InterruptedException,
            BucketAccessException, TimeoutException {
        dynamodbTestInterface = new DynamodbTestInterface(LOCAL_DYNAMO, NETWORK);
    }

    @AfterEach
    void afterEach() {
        dynamodbTestInterface.deleteCreatedTables();
    }

    @AfterAll
    static void afterAll() {
        NETWORK.close();
    }

    @Test
    void testSimplePrimaryKey() {
        final String keyName = "primary_key";
        dynamodbTestInterface.createTable(TABLE_NAME, keyName);
        final DynamodbTableMetadata dynamodbTableMetadata = new DynamodbTableMetadataFactory().forDynamodbTable(getDynamodbConnection(),
                TABLE_NAME);
        assertAll(//
                () -> assertThat(dynamodbTableMetadata.getPrimaryKey().getPartitionKey().toString(), equalTo("/" + keyName)),
                () -> assertThat(dynamodbTableMetadata.getPrimaryKey().hasSortKey(), equalTo(false))//
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
        final DynamodbTableMetadata dynamodbTableMetadata = new DynamodbTableMetadataFactory().forDynamodbTable(getDynamodbConnection(),
                TABLE_NAME);
        assertAll(//
                () -> assertThat(dynamodbTableMetadata.getPrimaryKey().getPartitionKey().toString(),
                        equalTo("/" + partitionKey)),
                () -> assertThat(dynamodbTableMetadata.getPrimaryKey().getSortKey().toString(), equalTo("/" + sortKey))//
        );
    }

    @Test
    void testLocalIndex() {
        final String partitionKey = "partition_key";
        final String sortKey = "sort_key";
        final String indexKey = "index_key";
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
                        .withIndexName("myIndex").withProjection(new Projection().withProjectionType("KEYS_ONLY")));
        dynamodbTestInterface.createTable(request);
        final DynamodbTableMetadata dynamodbTableMetadata = new DynamodbTableMetadataFactory().forDynamodbTable(getDynamodbConnection(),
                TABLE_NAME);
        final DynamodbKey index = dynamodbTableMetadata.getLocalIndexes().get(0);
        assertAll(//
                () -> assertThat(index.getPartitionKey().toString(), equalTo("/" + partitionKey)), //
                () -> assertThat(index.getSortKey().toString(), equalTo("/" + indexKey))//
        );
    }

    @Test
    void testGlobalIndex() {
        final String partitionKey = "partition_key";
        final String sortKey = "sort_key";
        final String indexKey1 = "index_key1";
        final String indexKey2 = "index_key2";
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
                        .withIndexName("myIndex").withProjection(new Projection().withProjectionType("KEYS_ONLY"))
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)));
        dynamodbTestInterface.createTable(request);
        final DynamodbTableMetadata dynamodbTableMetadata = new DynamodbTableMetadataFactory().forDynamodbTable(getDynamodbConnection(),
                TABLE_NAME);
        final DynamodbKey index = dynamodbTableMetadata.getGlobalIndexes().get(0);
        assertAll(//
                () -> assertThat(index.getPartitionKey().toString(), equalTo("/" + indexKey1)), //
                () -> assertThat(index.getSortKey().toString(), equalTo("/" + indexKey2))//
        );
    }
}