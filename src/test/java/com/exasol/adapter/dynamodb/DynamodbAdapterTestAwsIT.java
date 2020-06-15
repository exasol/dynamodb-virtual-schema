package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exasol.adapter.dynamodb.mapping.MappingTestFiles;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.dynamodb.DynamodbConnectionFactory;

/**
 * Tests using the AWS DynamoDB. Setup credentials on your machine using: {@code aws configure}.
 *
 */
@Tag("integration")
class DynamodbAdapterTestAwsIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbAdapterTestAwsIT.class);

    private static final String TEST_SCHEMA = "TEST";
    private static final String DYNAMODB_CONNECTION = "DYNAMODB_CONNECTION";
    private static AwsExasolTestInterface exasolTestInterface;

    /**
     * Create a Virtual Schema in the Exasol test container accessing DynamoDB on AWS.
     */
    @BeforeAll
    static void beforeAll() throws SQLException, BucketAccessException, InterruptedException,
            java.util.concurrent.TimeoutException, IOException {
        final DynamodbTestInterface dynamodbTestInterface = new DynamodbTestInterface();
        new OpenLibrary(dynamodbTestInterface);
        exasolTestInterface = new AwsExasolTestInterface();
        exasolTestInterface.uploadDynamodbAdapterJar();
        exasolTestInterface.uploadMapping(MappingTestFiles.OPEN_LIBRARY_MAPPING_FILE_NAME);
        Thread.sleep(1000 * 5);// waiting for bucketfs to sync
        exasolTestInterface.dropConnection(DYNAMODB_CONNECTION);
        exasolTestInterface.dropVirtualSchema(TEST_SCHEMA);
        exasolTestInterface.createConnection(DYNAMODB_CONNECTION, "aws:eu-central-1",
                dynamodbTestInterface.getDynamoUser(), DynamodbConnectionFactory.buildPassWithTokenSeparator(
                        dynamodbTestInterface.getDynamoPass(), dynamodbTestInterface.getSessionToken()));
        exasolTestInterface.createAdapterScript();
        exasolTestInterface.createUdf();
        exasolTestInterface.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                "/bfsdefault/default/mappings/" + MappingTestFiles.OPEN_LIBRARY_MAPPING_FILE_NAME);
    }

    @Test
    void testCountAllRows() throws SQLException {
        final ResultSet resultSet = exasolTestInterface.getStatement().executeQuery("SELECT COUNT(*) FROM OPENLIBRARY");
        resultSet.next();
        final int count = resultSet.getInt(1);
        assertThat(count, equalTo(148163));
    }

    @Test
    void testSelectSingleRow() throws SQLException {
        final ResultSet resultSet = exasolTestInterface.getStatement()
                .executeQuery("SELECT COUNT(*) FROM OPENLIBRARY WHERE KEY = '/authors/OL7124039A' AND REVISION = 1");
        resultSet.next();
        final int count = resultSet.getInt(1);
        assertThat(count, equalTo(1));
    }

    @AfterAll
    static void afterAll() throws SQLException {
        exasolTestInterface.dropConnection(DYNAMODB_CONNECTION);
        exasolTestInterface.dropVirtualSchema(TEST_SCHEMA);
    }
}
