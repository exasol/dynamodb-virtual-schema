package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
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
	public static final GenericContainer localDynamo = new GenericContainer<>("amazon/dynamodb-local").withExposedPorts(8000)
			.withNetwork(network).withNetworkAliases("dynamo")
			.withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");

	private static DynamodbTestUtils dynamodbTestUtils;
	private static ExasolTestUtils exasolTestUtils;

	private static final String TEST_SCHEMA = "TEST";
	private static final String DYNAMODB_CONNECTION = "DYNAMODB_CONNECTION";

	private static final String DYNAMO_TABLE_NAME = "JB_Books";

	@BeforeAll
	static void beforeAll() throws Exception {
		dynamodbTestUtils = new DynamodbTestUtils(localDynamo, network);
		exasolTestUtils = new ExasolTestUtils(exasolContainer);
		exasolTestUtils.uploadDynamodbAdapterJar();
		exasolTestUtils.createAdapterScript();
		LOGGER.info("created adapter script");
		exasolTestUtils.createConnection(DYNAMODB_CONNECTION, dynamodbTestUtils.getDynamoUrl(),
				dynamodbTestUtils.getDynamoUser(), dynamodbTestUtils.getDynamoPass());
		LOGGER.info("created connection");
		exasolTestUtils.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION);
		LOGGER.info("created schema");
	}

	@Test
	void testSingleLineSelect() throws SQLException {
		dynamodbTestUtils.createTable(DYNAMO_TABLE_NAME, "isbn");
		dynamodbTestUtils.pushItem();

		final ResultSet actualResult = exasolTestUtils.getStatement()
				.executeQuery("SELECT * FROM " + TEST_SCHEMA + ".\"testTable\";");// table name is hardcoded in adapter
																					// definition (DynamodbAdapter)
		assertNotNull(actualResult);
		assertTrue(actualResult.next());
		assertEquals("12398439493", actualResult.getString(1));
		assertFalse(actualResult.next());
	}

	@Test
	void testMultiLineSelect() throws IOException, InterruptedException, SQLException {
		dynamodbTestUtils.createTable(DYNAMO_TABLE_NAME, "isbn");

		final ClassLoader classLoader = DynamodbTestUtilsTestIT.class.getClassLoader();
		dynamodbTestUtils.importData(new File(classLoader.getResource("books.json").getFile()));

		final ResultSet actualResultSet = exasolTestUtils.getStatement()
				.executeQuery("SELECT * FROM " + TEST_SCHEMA + ".\"testTable\";");

		assertNotNull(actualResultSet);

		final List<String> actualResult = new ArrayList<>();
		while(actualResultSet.next()){
			actualResult.add(actualResultSet.getString(1));
		}

		assertThat(actualResult,containsInAnyOrder("123567", "123254545", "1235673"));
		assertEquals(3,actualResultSet.getRow());
	}

	@AfterEach
	void after(){
		dynamodbTestUtils.deleteCreatedTables();
	}

}
