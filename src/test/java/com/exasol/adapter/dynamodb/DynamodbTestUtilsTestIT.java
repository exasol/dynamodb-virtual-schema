package com.exasol.adapter.dynamodb;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import util.DynamodbTestUtils;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 *  Tests the {@link DynamodbTestUtils}
 */
@Tag("integration")
@Testcontainers
public class DynamodbTestUtilsTestIT {

    final static Network network = Network.newNetwork();

    @Container
    public static GenericContainer localDynamo = new GenericContainer<>("amazon/dynamodb-local")
            .withExposedPorts(8000).withNetwork(network).withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");

    private static DynamodbTestUtils dynamodbTestUtils;

    @BeforeAll
    static void beforeAll() throws Exception {
        dynamodbTestUtils = new DynamodbTestUtils(localDynamo,network);
        dynamodbTestUtils.createTable("JB_Books","isbn");
    }

    @Test
    void test1() throws IOException, InterruptedException {
        final ClassLoader classLoader = DynamodbTestUtilsTestIT.class.getClassLoader();
        final File books = new File(classLoader.getResource("books.json").getFile());
        dynamodbTestUtils.importData(books);
        assertEquals(1,dynamodbTestUtils.scan("JB_Books"));
    }
}
