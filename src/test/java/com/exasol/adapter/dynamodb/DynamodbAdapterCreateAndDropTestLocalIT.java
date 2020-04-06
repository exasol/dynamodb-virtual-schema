package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.ExaMetadata;
import com.exasol.adapter.request.CreateVirtualSchemaRequest;
import com.exasol.adapter.request.DropVirtualSchemaRequest;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;

/**
 * Tests the {@link DynamodbAdapter#createVirtualSchema(ExaMetadata, CreateVirtualSchemaRequest)} and
 * {@link DynamodbAdapter#dropVirtualSchema(ExaMetadata, DropVirtualSchemaRequest)} using a local docker version of
 * DynamoDB and a local docker version of exasol.
 **/
@Tag("integration")
@Testcontainers
public class DynamodbAdapterCreateAndDropTestLocalIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbAdapterCreateAndDropTestLocalIT.class);

    private static final Network NETWORK = Network.newNetwork();
    @Container
    public static final GenericContainer LOCAL_DYNAMO = new GenericContainer<>("amazon/dynamodb-local")
            .withExposedPorts(8000).withNetwork(NETWORK).withNetworkAliases("dynamo")
            .withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");
    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> EXASOL_CONTAINER = new ExasolContainer<>()
            .withNetwork(NETWORK).withExposedPorts(8888).withLogConsumer(new Slf4jLogConsumer(LOGGER));
    private static final String TEST_SCHEMA = "TEST";
    private static final String DYNAMODB_CONNECTION = "DYNAMODB_CONNECTION";
    private static ExasolTestUtils exasolTestUtils;

    /**
     * Creates a Virtual Schema in the Exasol test container accessing the local DynamoDB.
     */
    @BeforeAll
    static void beforeAll() throws DynamodbTestUtils.NoNetworkFoundException, SQLException, InterruptedException,
            BucketAccessException, TimeoutException {
        final DynamodbTestUtils dynamodbTestUtils = new DynamodbTestUtils(LOCAL_DYNAMO, NETWORK);
        exasolTestUtils = new ExasolTestUtils(EXASOL_CONTAINER);
        exasolTestUtils.uploadDynamodbAdapterJar();
        exasolTestUtils.createAdapterScript();
        exasolTestUtils.createConnection(DYNAMODB_CONNECTION, dynamodbTestUtils.getDynamoUrl(),
                dynamodbTestUtils.getDynamoUser(), dynamodbTestUtils.getDynamoPass());
    }

    @AfterAll
    static void afterAll() {
        NETWORK.close();
    }

    @Test
    public void testCreateAndDrop() throws SQLException, InterruptedException, BucketAccessException, TimeoutException {
        exasolTestUtils.uploadMapping("basicMapping.json", "mappings/test.json");
        exasolTestUtils.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                "/bfsdefault/default/mappings/test.json");
        assertThat(exasolTestUtils.testIfSchemaExists(TEST_SCHEMA), equalTo(true));
        exasolTestUtils.dropVirtualSchema(TEST_SCHEMA);
        assertThat(exasolTestUtils.testIfSchemaExists(TEST_SCHEMA), equalTo(false));
    }
}
