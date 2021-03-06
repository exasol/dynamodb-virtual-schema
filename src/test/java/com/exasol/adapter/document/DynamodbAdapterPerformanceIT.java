package com.exasol.adapter.document;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.mapping.MappingTestFiles;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.dynamodb.DynamodbConnectionFactory;

/**
 * Tests using the AWS DynamoDB. Setup credentials on your machine using: {@code aws configure}.
 */
@Tag("integration")
@SuppressWarnings("java:S2699") // tests in this class do not contains assertions because they are performance tests
class DynamodbAdapterPerformanceIT {
    private static final String TEST_SCHEMA = "TEST";
    private static final String DYNAMODB_CONNECTION = "DYNAMODB_CONNECTION";
    private static ExasolTestInterface exasolTestInterface;
    private static DynamodbTestInterface dynamodbTestInterface;
    private static DynamodbVsExasolTestDatabaseBuilder exasolTestDatabaseBuilder;

    /**
     * Create a Virtual Schema in the Exasol test container accessing DynamoDB on AWS.
     */
    @BeforeAll
    static void beforeAll() throws SQLException, BucketAccessException, InterruptedException,
            java.util.concurrent.TimeoutException, IOException, NoSuchAlgorithmException, KeyManagementException,
            DynamodbTestInterface.NoNetworkFoundException, URISyntaxException {
        final IntegrationTestSetup integrationTestSetup = new IntegrationTestSetup();
        dynamodbTestInterface = integrationTestSetup.getDynamodbTestInterface();
        exasolTestInterface = integrationTestSetup.getExasolTestInterface();
        exasolTestDatabaseBuilder = new DynamodbVsExasolTestDatabaseBuilder(exasolTestInterface);
        exasolTestDatabaseBuilder.uploadDynamodbAdapterJar();
        exasolTestDatabaseBuilder.uploadMappingTestFile(MappingTestFiles.OPEN_LIBRARY_MAPPING,
                MappingTestFiles.OPEN_LIBRARY_MAPPING);
        Thread.sleep(1000 * 5);// waiting for bucketfs to sync
        exasolTestDatabaseBuilder.dropConnection(DYNAMODB_CONNECTION);
        exasolTestDatabaseBuilder.dropVirtualSchema(TEST_SCHEMA);
        exasolTestDatabaseBuilder.createConnection(DYNAMODB_CONNECTION, "aws:eu-central-1",
                dynamodbTestInterface.getDynamoUser(), DynamodbConnectionFactory.buildPassWithTokenSeparator(
                        dynamodbTestInterface.getDynamoPass(), dynamodbTestInterface.getSessionToken()));
        exasolTestDatabaseBuilder.createAdapterScript();
        exasolTestDatabaseBuilder.createUdf();
        exasolTestDatabaseBuilder.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                "/bfsdefault/default/" + MappingTestFiles.OPEN_LIBRARY_MAPPING);
    }

    @AfterAll
    static void afterAll() {
        exasolTestInterface.teardown();
        dynamodbTestInterface.teardown();
    }

    @Test
    void testCountAllRowsWith2Columns() throws SQLException {
        final ResultSet resultSet = exasolTestDatabaseBuilder.getStatement()
                .executeQuery("SELECT \"KEY\", \"REVISION\" FROM OPENLIBRARY");
        resultSet.next();
    }

    @Test
    void testCountAllRowsWith3Columns() throws SQLException {
        final ResultSet resultSet = exasolTestDatabaseBuilder.getStatement()
                .executeQuery("SELECT \"KEY\", \"REVISION\", \"TITLE\" FROM OPENLIBRARY");
        resultSet.next();
    }

    @Test
    void testCountAllRowsWith4Columns() throws SQLException {
        final ResultSet resultSet = exasolTestDatabaseBuilder.getStatement()
                .executeQuery("SELECT \"KEY\", \"REVISION\", \"TITLE\", \"TITLE_PREFIX\" FROM OPENLIBRARY");
        resultSet.next();
    }

    @Test
    void testToJson() throws SQLException {
        final ResultSet resultSet = exasolTestDatabaseBuilder.getStatement()
                .executeQuery("SELECT \"PUBLISHERS\" FROM OPENLIBRARY");
        resultSet.next();
    }

    @Test
    void testSelectSingleRow() throws SQLException {
        final ResultSet resultSet = exasolTestDatabaseBuilder.getStatement().executeQuery(
                "SELECT COUNT(\"KEY\") FROM OPENLIBRARY WHERE KEY = '/authors/OL13141A' AND REVISION = 1");
        resultSet.next();
        final int count = resultSet.getInt(1);
        assertThat(count, equalTo(1));
    }

    @Test
    void testCountAuthorsTable() throws SQLException {
        final ResultSet resultSet = exasolTestDatabaseBuilder.getStatement()
                .executeQuery("SELECT \"KEY\" FROM OPENLIBRARY_AUTHORS");
        resultSet.next();
    }
}
