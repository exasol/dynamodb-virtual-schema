package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.ExaMetadata;
import com.exasol.adapter.dynamodb.mapping.MappingTestFiles;
import com.exasol.adapter.request.RefreshRequest;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;

/**
 * Tests the {@link DynamodbAdapter#refresh(ExaMetadata, RefreshRequest)} using a local docker version of DynamoDB and a
 * local docker version of exasol.
 **/
@Tag("integration")
@Testcontainers
class DynamodbAdapterRefreshTestLocalIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbAdapterRefreshTestLocalIT.class);

    private static final Network NETWORK = Network.newNetwork();
    @Container
    public static final GenericContainer LOCAL_DYNAMO = new GenericContainer<>("amazon/dynamodb-local")
            .withExposedPorts(8000).withNetwork(NETWORK).withNetworkAliases("dynamo")
            .withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");
    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> EXASOL_CONTAINER = new ExasolContainer<>()
            .withNetwork(NETWORK).withExposedPorts(8888).withLogConsumer(new Slf4jLogConsumer(LOGGER));
    private static final String TEST_SCHEMA = "TEST";
    private static final String BOOKS_TABLE = "BOOKS";
    private static final String DYNAMODB_CONNECTION = "DYNAMODB_CONNECTION";
    private static final String BUCKETFS_MAPPING_FILE_NAME = "mappings/test.json";
    private static final String BUCKETFS_MAPPING_FULL_PATH = "/bfsdefault/default/" + BUCKETFS_MAPPING_FILE_NAME;
    private static DynamodbTestInterface dynamodbTestInterface;
    private static ExasolTestInterface exasolTestInterface;

    /**
     * Creates a Virtual Schema in the Exasol test container accessing the local DynamoDB.
     */
    @BeforeAll
    static void beforeAll() throws DynamodbTestInterface.NoNetworkFoundException, SQLException, InterruptedException,
            BucketAccessException, TimeoutException, IOException {
        dynamodbTestInterface = new DynamodbTestInterface(LOCAL_DYNAMO, NETWORK);
        exasolTestInterface = new ExasolTestInterface(EXASOL_CONTAINER);
        exasolTestInterface.uploadDynamodbAdapterJar();
        exasolTestInterface.createAdapterScript();
        exasolTestInterface.createConnection(DYNAMODB_CONNECTION, dynamodbTestInterface.getDynamoUrl(),
                dynamodbTestInterface.getDynamoUser(), dynamodbTestInterface.getDynamoPass());
    }

    @AfterAll
    static void afterAll() {
        NETWORK.close();
    }

    @AfterEach
    void after() throws SQLException {
        exasolTestInterface.dropVirtualSchema(TEST_SCHEMA);
    }

    /**
     * In this test case the schema mapping is replaced but {@code REFRESH} is not called. Thus the virtual schema
     * should not change.
     */
    @Test
    void testSchemaDefinitionDoesNotChangeUntilRefresh()
            throws SQLException, InterruptedException, BucketAccessException, TimeoutException {
        exasolTestInterface.uploadMapping(MappingTestFiles.BASIC_MAPPING_FILE_NAME, BUCKETFS_MAPPING_FILE_NAME);
        exasolTestInterface.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION, BUCKETFS_MAPPING_FULL_PATH);
        final Map<String, String> columnsBefore = exasolTestInterface.describeTable(TEST_SCHEMA, BOOKS_TABLE);
        exasolTestInterface.uploadMapping(MappingTestFiles.TO_JSON_MAPPING_FILE_NAME, BUCKETFS_MAPPING_FILE_NAME);
        final Map<String, String> columnsAfter = exasolTestInterface.describeTable(TEST_SCHEMA, BOOKS_TABLE);
        assertThat(columnsBefore, equalTo(columnsAfter));
    }

    /**
     * In this test case the schema mapping is replaced and {@code REFRESH} is called. Thus the virtual schema should
     * have change.
     */
    @Test
    void testSchemaDefinitionChangesOnRefresh()
            throws SQLException, InterruptedException, BucketAccessException, TimeoutException {
        exasolTestInterface.uploadMapping(MappingTestFiles.BASIC_MAPPING_FILE_NAME, BUCKETFS_MAPPING_FILE_NAME);
        exasolTestInterface.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION, BUCKETFS_MAPPING_FULL_PATH);
        final Map<String, String> columnsBefore = exasolTestInterface.describeTable(TEST_SCHEMA, BOOKS_TABLE);
        exasolTestInterface.uploadMapping(MappingTestFiles.TO_JSON_MAPPING_FILE_NAME, BUCKETFS_MAPPING_FILE_NAME);
        Thread.sleep(5000);// Wait for bucketfs to sync; Quick fix for
                           // https://github.com/exasol/exasol-testcontainers/issues/54
        exasolTestInterface.refreshVirtualSchema(TEST_SCHEMA);
        final Map<String, String> columnsAfter = exasolTestInterface.describeTable(TEST_SCHEMA, BOOKS_TABLE);
        assertThat(columnsBefore, not(equalTo(columnsAfter)));
    }
}
