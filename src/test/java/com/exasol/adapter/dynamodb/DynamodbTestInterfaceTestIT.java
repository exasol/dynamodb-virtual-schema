package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.adapter.dynamodb.mapping.TestDocuments;

/**
 * Tests the {@link DynamodbTestInterface}.
 */
@Tag("integration")
@Testcontainers
public class DynamodbTestInterfaceTestIT {
    private static final Network NETWORK = Network.newNetwork();
    @Container
    public static final GenericContainer LOCAL_DYNAMO = new GenericContainer<>("amazon/dynamodb-local")
            .withExposedPorts(8000).withNetwork(NETWORK).withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");
    private static final String TABLE_NAME = "TEST";
    private static DynamodbTestInterface dynamodbTestInterface;

    @BeforeAll
    static void beforeAll() throws DynamodbTestInterface.NoNetworkFoundException {
        dynamodbTestInterface = new DynamodbTestInterface(LOCAL_DYNAMO, NETWORK);
    }

    @AfterAll
    static void afterAll() {
        NETWORK.close();
    }

    @AfterEach
    void after() {
        dynamodbTestInterface.deleteCreatedTables();
    }

    @Test
    void testImportData() throws IOException {
        dynamodbTestInterface.createTable(TABLE_NAME, TestDocuments.BOOKS_ISBN_PROPERTY);
        final ClassLoader classLoader = DynamodbTestInterfaceTestIT.class.getClassLoader();
        dynamodbTestInterface.importData(TABLE_NAME, TestDocuments.BOOKS);
        assertThat(dynamodbTestInterface.scan(TABLE_NAME), equalTo(3));
    }

    @Test
    void testPutJson() {
        dynamodbTestInterface.createTable(TABLE_NAME, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestInterface.putJson(TABLE_NAME, "{\n" + "\"isbn\": \"1234\",\n" + " \"name\":\"book1\"\n" + "}");
        assertThat(dynamodbTestInterface.scan(TABLE_NAME), equalTo(1));
    }
}
