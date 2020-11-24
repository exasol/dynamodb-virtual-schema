package com.exasol.adapter.document;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.net.URISyntaxException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;

import com.exasol.dynamodb.DynamodbContainer;

/**
 * Tests the {@link DynamodbTestDbBuilder}.
 */
@Tag("integration")
@Tag("quick")
class DynamodbTestDbBuilderIT {
    private static final String TABLE_NAME = "TEST";
    @Container
    private static final DynamodbContainer DYNAMODB = new DynamodbContainer();
    private static DynamodbTestDbBuilder dynamodbTestDbBuilder;

    @BeforeAll
    static void beforeAll() throws DynamodbTestDbBuilder.NoNetworkFoundException, IOException, URISyntaxException {
        dynamodbTestDbBuilder = new TestcontainerDynamodbTestDbBuilder(DYNAMODB);
    }

    @AfterEach
    void after() {
        dynamodbTestDbBuilder.deleteCreatedTables();
    }

    @Test
    void testImportData() throws IOException {
        dynamodbTestDbBuilder.createTable(TABLE_NAME, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestDbBuilder.importData(TABLE_NAME, TestDocuments.books());
        assertThat(dynamodbTestDbBuilder.scan(TABLE_NAME), equalTo(3));
    }

    @Test
    void testPutJson() {
        dynamodbTestDbBuilder.createTable(TABLE_NAME, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestDbBuilder.putJson(TABLE_NAME, "{\n" + "\"isbn\": \"1234\",\n" + " \"name\":\"book1\"\n" + "}");
        assertThat(dynamodbTestDbBuilder.scan(TABLE_NAME), equalTo(1));
    }
}
