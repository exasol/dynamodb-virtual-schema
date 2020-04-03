package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

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
import com.exasol.adapter.request.RefreshRequest;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;

/**
 * Tests the {@link DynamodbAdapter#refresh(ExaMetadata, RefreshRequest)} using
 * a local docker version of DynamoDB and a local docker version of exasol.
 **/
@Tag("integration")
@Testcontainers
public class DynamodbAdapterRefreshTestLocalIT {
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
	private static final String DYNAMODB_CONNECTION = "DYNAMODB_CONNECTION";
	private static DynamodbTestUtils dynamodbTestUtils;
	private static ExasolTestUtils exasolTestUtils;

	/**
	 * Creates a Virtual Schema in the Exasol test container accessing the local
	 * DynamoDB.
	 */
	@BeforeAll
	static void beforeAll() throws DynamodbTestUtils.NoNetworkFoundException, SQLException, InterruptedException,
			BucketAccessException, TimeoutException {
		dynamodbTestUtils = new DynamodbTestUtils(LOCAL_DYNAMO, NETWORK);
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

	/**
	 * In this test case the schema mapping is replaced but {@code REFRESH} is not
	 * called. Thus the virtual schema should not change.
	 */
	@Test
	public void testSchemaDefinitionDoesNotChangeUntilRefresh()
			throws SQLException, InterruptedException, BucketAccessException, TimeoutException {
		exasolTestUtils.uploadMapping("basicMapping.json", "mappings/test.json");
		exasolTestUtils.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
				"/bfsdefault/default/mappings/test.json");
		final Map<String, String> columnsBefore = exasolTestUtils.describeTable(TEST_SCHEMA, "BOOKS");
		exasolTestUtils.uploadMapping("toJsonMapping.json", "mappings/test.json");
		final Map<String, String> columnsAfter = exasolTestUtils.describeTable(TEST_SCHEMA, "BOOKS");
		assertThat(columnsBefore, equalTo(columnsAfter));
	}

	/**
	 * In this test case the schema mapping is replaced and {@code REFRESH} is
	 * called. Thus the virtual schema should have change.
	 */
	@Test
	public void testSchemaDefinitionChangesOnRefresh()
			throws SQLException, InterruptedException, BucketAccessException, TimeoutException {
		exasolTestUtils.uploadMapping("basicMapping.json", "mappings/test.json");
		exasolTestUtils.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
				"/bfsdefault/default/mappings/test.json");
		final Map<String, String> columnsBefore = exasolTestUtils.describeTable(TEST_SCHEMA, "BOOKS");
		exasolTestUtils.uploadMapping("toJsonMapping.json", "mappings/test.json");
		Thread.sleep(5000);// Wait for bucketfs to sync //NOSONAR
		exasolTestUtils.refreshVirtualSchema(TEST_SCHEMA);
		final Map<String, String> columnsAfter = exasolTestUtils.describeTable(TEST_SCHEMA, "BOOKS");
		assertThat(columnsBefore, not(equalTo(columnsAfter)));
	}

	@AfterEach
	void after() throws SQLException {
		exasolTestUtils.dropVirtualSchema(TEST_SCHEMA);
	}
}