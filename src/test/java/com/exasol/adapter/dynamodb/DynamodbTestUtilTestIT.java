package com.exasol.adapter.dynamodb;

import com.exasol.bucketfs.BucketAccessException;
import com.exasol.jdbc.TimeoutException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import util.DynamodbTestUtils;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

@Tag("integration")
@Testcontainers
public class DynamodbTestUtilTestIT {
    @Container
    public static GenericContainer localDynamo = new GenericContainer<>("amazon/dynamodb-local")
            .withExposedPorts(8000).withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");

    private static DynamodbTestUtils dynamodbTestUtils;

    @BeforeAll
    static void beforeAll() throws SQLException, BucketAccessException, InterruptedException, TimeoutException, java.util.concurrent.TimeoutException {
        dynamodbTestUtils = new DynamodbTestUtils(localDynamo);
        dynamodbTestUtils.createTable("JB_Books","isbn");
        //dynamodbTestUtils.pushItem();
    }

    @Test
    void test1() throws IOException, InterruptedException {
        ClassLoader classLoader = DynamodbTestUtilTestIT.class.getClassLoader();
        File books = new File(classLoader.getResource("books.json").getFile());
        dynamodbTestUtils.importData(books);
        assertEquals(1,dynamodbTestUtils.scan("JB_Books"));
    }
}
