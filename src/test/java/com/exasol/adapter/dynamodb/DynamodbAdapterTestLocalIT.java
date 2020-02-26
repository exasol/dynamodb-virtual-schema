package com.exasol.adapter.dynamodb;

import com.exasol.bucketfs.BucketAccessException;
import com.exasol.jdbc.TimeoutException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolContainerConstants;
import util.DynamodbTestUtils;
import util.ExasolTestUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;
/**
   Tests the {@link DynamodbAdapter} using a local docker version of DynamoDB and a local docker version of exasol.
 **/
@Tag("integration")
@Testcontainers
public class DynamodbAdapterTestLocalIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbAdapterTestLocalIT.class);

    @Container
    private static ExasolContainer<? extends ExasolContainer<?>> exasolContainer = new ExasolContainer<>(
            ExasolContainerConstants.EXASOL_DOCKER_IMAGE_REFERENCE).withLogConsumer(new Slf4jLogConsumer(LOGGER));

    @Container
    public static GenericContainer localDynamo = new GenericContainer<>("amazon/dynamodb-local")
            .withExposedPorts(8000).withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");

    private static DynamodbTestUtils dynamodbTestUtils;
    private static ExasolTestUtils exasolTestUtils;

    private static final String TEST_SCHEMA = "TEST";
    private static final String DYNAMODB_CONNECTION = "DYNAMODB_CONNECTION";

    private static final String DYNAMO_TABLE_NAME = "JB_Books";



    @BeforeAll
    static void beforeAll() throws SQLException, BucketAccessException, InterruptedException, TimeoutException, java.util.concurrent.TimeoutException {
        dynamodbTestUtils = new DynamodbTestUtils(localDynamo);
        exasolTestUtils = new ExasolTestUtils(exasolContainer);
        exasolTestUtils.uploadDynamodbAdapterJar();
        exasolTestUtils.createAdapterScript();
        exasolTestUtils.createConnection(DYNAMODB_CONNECTION, dynamodbTestUtils.getUrl(), DynamodbTestUtils.LOCAL_DYNAMO_USER,DynamodbTestUtils.LOCAL_DYNAMO_PASS);
        exasolTestUtils.createDynamodbVirtualSchema(TEST_SCHEMA,DYNAMODB_CONNECTION);

        //create dummy data
        dynamodbTestUtils.createTable(DYNAMO_TABLE_NAME, "isbn");
        dynamodbTestUtils.pushItem();
    }

    @Test
    void testSelect() throws SQLException {
        final ResultSet expected = exasolTestUtils.getStatement().executeQuery("SELECT * FROM " +  TEST_SCHEMA + ".\"testTable\";");//table name is hardcoded in adapter definition (DynamodbAdapter)
        assertNotNull(expected);
        assertTrue(expected.next());
        assertEquals("12398439493", expected.getString(1));
        assertFalse(expected.next());
    }


}
