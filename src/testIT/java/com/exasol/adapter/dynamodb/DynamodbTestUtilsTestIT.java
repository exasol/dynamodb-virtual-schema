package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Tests the {@link DynamodbTestUtils}.
 */
@Tag("integration")
@Testcontainers
public class DynamodbTestUtilsTestIT {
	private static final Network NETWORK = Network.newNetwork();
	@Container
	public static final GenericContainer LOCAL_DYNAMO = new GenericContainer<>("amazon/dynamodb-local")
			.withExposedPorts(8000).withNetwork(NETWORK).withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");
	private static final String TABLE_NAME = "TEST";
	private static DynamodbTestUtils dynamodbTestUtils;

	@BeforeAll
	static void beforeAll() throws DynamodbTestUtils.NoNetworkFoundException {
		dynamodbTestUtils = new DynamodbTestUtils(LOCAL_DYNAMO, NETWORK);
	}

	@AfterAll
	static void afterAll() {
		NETWORK.close();
	}

	/**
	 * Test for {@link DynamodbTestUtils#importData(String, File)}
	 */
	@Test
	void testImportData() throws IOException {
		dynamodbTestUtils.createTable(TABLE_NAME, "isbn");
		final ClassLoader classLoader = DynamodbTestUtilsTestIT.class.getClassLoader();
		final File books = new File(classLoader.getResource("books.json").getFile());
		dynamodbTestUtils.importData(TABLE_NAME, books);
		assertThat(dynamodbTestUtils.scan(TABLE_NAME), equalTo(3));
	}

	@Test
	void testPutJson() {
		dynamodbTestUtils.createTable(TABLE_NAME, "isbn");
		dynamodbTestUtils.putJson(TABLE_NAME, "{\n" + "\"isbn\": \"1234\",\n" + " \"name\":\"book1\"\n" + "}");
		assertThat(dynamodbTestUtils.scan(TABLE_NAME), equalTo(1));
	}

	@AfterEach
	void after() {
		dynamodbTestUtils.deleteCreatedTables();
	}
}
