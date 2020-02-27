package com.exasol.adapter.dynamodb;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.SQLException;

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

import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolContainerConstants;

import util.DynamodbTestUtils;
import util.ExasolTestUtils;

/**
 * Tests the {@link DynamodbAdapter} using a local docker version of DynamoDB
 * and a local docker version of exasol.
 **/
@Tag("integration")
@Testcontainers
public class DynamodbAdapterTestLocalIT {
	private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbAdapterTestLocalIT.class);

	final static Network network = Network.newNetwork();

	@Container
	private static final ExasolContainer<? extends ExasolContainer<?>> exasolContainer = new ExasolContainer<>(
			ExasolContainerConstants.EXASOL_DOCKER_IMAGE_REFERENCE).withNetwork(network)
					.withLogConsumer(new Slf4jLogConsumer(LOGGER));

	@Container
	public static GenericContainer localDynamo = new GenericContainer<>("amazon/dynamodb-local").withExposedPorts(8000)
			.withNetwork(network).withNetworkAliases("dynamo")
			.withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");

	private static DynamodbTestUtils dynamodbTestUtils;
	private static ExasolTestUtils exasolTestUtils;

	private static final String TEST_SCHEMA = "TEST";
	private static final String DYNAMODB_CONNECTION = "DYNAMODB_CONNECTION";

	private static final String DYNAMO_TABLE_NAME = "JB_Books";

	@BeforeAll
	static void beforeAll() throws Exception {
		LOGGER.info("starting locat test beforAll");
		dynamodbTestUtils = new DynamodbTestUtils(localDynamo, network);
		LOGGER.info("inited dynamoTestUtil");
		exasolTestUtils = new ExasolTestUtils(exasolContainer);
		LOGGER.info("inited exasolTestUtil");
		exasolTestUtils.uploadDynamodbAdapterJar();
		LOGGER.info("uploaded jar");
		exasolTestUtils.createAdapterScript();
		LOGGER.info("created adapter script");
		exasolTestUtils.createConnection(DYNAMODB_CONNECTION, dynamodbTestUtils.getDynamoUrl(),
				dynamodbTestUtils.getDynamoUser(), dynamodbTestUtils.getDynamoPass());
		LOGGER.info("created connection");
		exasolTestUtils.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION);
		LOGGER.info("created schema");
		// create dummy data
		dynamodbTestUtils.createTable(DYNAMO_TABLE_NAME, "isbn");
		LOGGER.info("created table");
		dynamodbTestUtils.pushItem();
		LOGGER.info("created item");
	}

	@Test
	void testSelect() throws SQLException {
		final ResultSet expected = exasolTestUtils.getStatement()
				.executeQuery("SELECT * FROM " + TEST_SCHEMA + ".\"testTable\";");// table name is hardcoded in adapter
																					// definition (DynamodbAdapter)
		assertNotNull(expected);
		assertTrue(expected.next());
		assertEquals("12398439493", expected.getString(1));
		assertFalse(expected.next());
	}

}
