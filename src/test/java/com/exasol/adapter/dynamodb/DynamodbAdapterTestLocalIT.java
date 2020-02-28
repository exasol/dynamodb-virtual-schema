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

	private static final class SelectStringArrayResult{
		public SelectStringArrayResult(final List<String> rows, final long duration){
			this.rows = rows;
			this.duration = duration;
		}
		public final List<String> rows;
		public final long duration;
	}

	private SelectStringArrayResult selectStringArray() throws SQLException {
		final long start = System.currentTimeMillis();
		final ResultSet actualResultSet = exasolTestUtils.getStatement()
				.executeQuery("SELECT * FROM " + TEST_SCHEMA + ".\"testTable\";");

		final long duration  = System.currentTimeMillis() -start;
		assertNotNull(actualResultSet);

		final List<String> result = new ArrayList<>();
		while(actualResultSet.next()){
			result.add(actualResultSet.getString(1));
		}

		LOGGER.info("query execution time was: " + String.valueOf(duration));
		return new SelectStringArrayResult(result, duration);
	}

	@Test
	void testEmptySelect() throws SQLException {
		dynamodbTestUtils.createTable(DYNAMO_TABLE_NAME, "isbn");
		final List<String> result = selectStringArray().rows;
		assertEquals(0,result.size());
	}

	@Test
	void testSingleLineSelect() throws SQLException {
		dynamodbTestUtils.createTable(DYNAMO_TABLE_NAME, "isbn");
		final String ISBN = "12398439493";
		dynamodbTestUtils.pushBook(ISBN, "test name");

		final ResultSet actualResult = exasolTestUtils.getStatement()
				.executeQuery("SELECT * FROM " + TEST_SCHEMA + ".\"testTable\";");// table name is hardcoded in adapter
																					// definition (DynamodbAdapter)
		assertNotNull(actualResult);
		assertTrue(actualResult.next());
		assertEquals(ISBN, actualResult.getString(1));
		assertFalse(actualResult.next());
	}

	@Test
	void testMultiLineSelect() throws IOException, InterruptedException, SQLException {
		dynamodbTestUtils.createTable(DYNAMO_TABLE_NAME, "isbn");

		final ClassLoader classLoader = DynamodbTestUtilsTestIT.class.getClassLoader();
		dynamodbTestUtils.importData(new File(classLoader.getResource("books.json").getFile()));

		final List<String> result = selectStringArray().rows;
		assertThat(result,containsInAnyOrder("123567", "123254545", "1235673"));
		assertEquals(3,result.size());
	}

	@Test
	void testBigScan() throws SQLException {
		dynamodbTestUtils.createTable(DYNAMO_TABLE_NAME, "isbn");
		final int numBooks = 1000;
		final List<String> actualBookNames = new ArrayList<>(numBooks);

		for(int i = 0; i < numBooks; i++){
			final String booksName = String.valueOf(i);
			dynamodbTestUtils.pushBook(booksName, "name equal for all books");
			actualBookNames.add(booksName);
		}

		final SelectStringArrayResult result = selectStringArray();
		assertEquals(numBooks,result.rows.size());
		assertThat(result.rows,containsInAnyOrder(actualBookNames.toArray()));
	}

	@AfterEach
	void after(){
		dynamodbTestUtils.deleteCreatedTables();
	}

}
