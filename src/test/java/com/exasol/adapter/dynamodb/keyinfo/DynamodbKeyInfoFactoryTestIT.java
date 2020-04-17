package com.exasol.adapter.dynamodb.keyinfo;

import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;

import com.exasol.adapter.dynamodb.DynamodbTestInterface;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.dynamodb.DynamodbConnectionFactory;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration")
@Testcontainers
class DynamodbKeyInfoFactoryTestIT {
    private static final Network NETWORK = Network.newNetwork();
    @Container
    public static final GenericContainer LOCAL_DYNAMO = new GenericContainer<>("amazon/dynamodb-local")
            .withNetwork(NETWORK).withExposedPorts(8000).withNetworkAliases("dynamo")
            .withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");
    private static DynamodbTestInterface dynamodbTestInterface;

    private static final String TABLE_NAME = "test";

    private AmazonDynamoDB getDynamodbConnection(){
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
    void test() {
        final String keyName = "primary_key";
        dynamodbTestInterface.createTable(TABLE_NAME, keyName);
        new DynamodbKeyInfoFactory().forDynamodbTable(getDynamodbConnection(), TABLE_NAME);
    }
}