package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.xmlrpc.XmlRpcException;
import org.junit.jupiter.api.*;

import com.exasol.ExaMetadata;
import com.exasol.adapter.dynamodb.mapping.MappingTestFiles;
import com.exasol.adapter.request.RefreshRequest;
import com.exasol.bucketfs.BucketAccessException;

/**
 * Tests the {@link DynamodbAdapter#refresh(ExaMetadata, RefreshRequest)} using a local docker version of DynamoDB and a
 * local docker version of exasol.
 **/
@Tag("integration")
class DynamodbAdapterRefreshIT {
    private static final String TEST_SCHEMA = "TEST";
    private static final String BOOKS_TABLE = "BOOKS";
    private static final String DYNAMODB_CONNECTION = "DYNAMODB_CONNECTION";
    private static final String BUCKETFS_MAPPING_FILE_NAME = "mappings/test.json";
    private static final String BUCKETFS_MAPPING_FULL_PATH = "/bfsdefault/default/" + BUCKETFS_MAPPING_FILE_NAME;
    private static ExasolTestInterface exasolTestInterface;
    private static DynamodbTestInterface dynamodbTestInterface;
    private static ExasolTestDatabaseBuilder exasolTestDatabaseBuilder;

    /**
     * Create a Virtual Schema in the Exasol test container accessing the local DynamoDB.
     */
    @BeforeAll
    static void beforeAll() throws DynamodbTestInterface.NoNetworkFoundException, SQLException, InterruptedException,
            BucketAccessException, TimeoutException, IOException, NoSuchAlgorithmException, KeyManagementException,
            XmlRpcException, URISyntaxException {
        final IntegrationTestSetup integrationTestSetup = new IntegrationTestSetup();
        exasolTestInterface = integrationTestSetup.getExasolTestInterface();
        exasolTestDatabaseBuilder = new ExasolTestDatabaseBuilder(exasolTestInterface);
        exasolTestDatabaseBuilder.uploadDynamodbAdapterJar();
        exasolTestDatabaseBuilder.createAdapterScript();
        dynamodbTestInterface = integrationTestSetup.getDynamodbTestInterface();
        exasolTestDatabaseBuilder.createConnection(DYNAMODB_CONNECTION, dynamodbTestInterface.getDynamoUrl(),
                dynamodbTestInterface.getDynamoUser(), dynamodbTestInterface.getDynamoPass());
    }

    @AfterAll
    static void afterAll() {
        exasolTestInterface.teardown();
        dynamodbTestInterface.teardown();
    }

    @AfterEach
    void after() throws SQLException {
        exasolTestDatabaseBuilder.dropVirtualSchema(TEST_SCHEMA);
    }

    /**
     * In this test case the schema mapping is replaced but {@code REFRESH} is not called. Thus the virtual schema
     * should not change.
     */
    @Test
    void testSchemaDefinitionDoesNotChangeUntilRefresh()
            throws SQLException, InterruptedException, BucketAccessException, TimeoutException {
        exasolTestDatabaseBuilder.uploadMapping(MappingTestFiles.BASIC_MAPPING_FILE_NAME, BUCKETFS_MAPPING_FILE_NAME);
        exasolTestDatabaseBuilder.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                BUCKETFS_MAPPING_FULL_PATH);
        final Map<String, String> columnsBefore = exasolTestDatabaseBuilder.describeTable(TEST_SCHEMA, BOOKS_TABLE);
        exasolTestDatabaseBuilder.uploadMapping(MappingTestFiles.TO_JSON_MAPPING_FILE_NAME, BUCKETFS_MAPPING_FILE_NAME);
        final Map<String, String> columnsAfter = exasolTestDatabaseBuilder.describeTable(TEST_SCHEMA, BOOKS_TABLE);
        assertThat(columnsBefore, equalTo(columnsAfter));
    }

    /**
     * In this test case the schema mapping is replaced and {@code REFRESH} is called. Thus the virtual schema should
     * have change.
     */
    @Test
    void testSchemaDefinitionChangesOnRefresh()
            throws SQLException, InterruptedException, BucketAccessException, TimeoutException {
        exasolTestDatabaseBuilder.uploadMapping(MappingTestFiles.BASIC_MAPPING_FILE_NAME, BUCKETFS_MAPPING_FILE_NAME);
        Thread.sleep(5000);
        exasolTestDatabaseBuilder.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                BUCKETFS_MAPPING_FULL_PATH);
        final Map<String, String> columnsBefore = exasolTestDatabaseBuilder.describeTable(TEST_SCHEMA, BOOKS_TABLE);
        exasolTestDatabaseBuilder.uploadMapping(MappingTestFiles.TO_JSON_MAPPING_FILE_NAME, BUCKETFS_MAPPING_FILE_NAME);
        Thread.sleep(5000);// Wait for bucketfs to sync; Quick fix for
                           // https://github.com/exasol/exasol-testcontainers/issues/54
        exasolTestDatabaseBuilder.refreshVirtualSchema(TEST_SCHEMA);
        final Map<String, String> columnsAfter = exasolTestDatabaseBuilder.describeTable(TEST_SCHEMA, BOOKS_TABLE);
        assertThat(columnsBefore, not(equalTo(columnsAfter)));
    }
}
