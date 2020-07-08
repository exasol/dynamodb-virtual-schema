package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.mapping.TestDocuments;

/**
 * Tests the {@link DynamodbTestInterface}.
 */
class DynamodbTestInterfaceTestIT {
    private static final String TABLE_NAME = "TEST";
    private static DynamodbTestInterface dynamodbTestInterface;

    @BeforeAll
    static void beforeAll() throws DynamodbTestInterface.NoNetworkFoundException, IOException {
        dynamodbTestInterface = new IntegrationTestSetup().getDynamodbTestInterface();
    }

    @AfterAll
    static void afterAll() {
        dynamodbTestInterface.teardown();
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
