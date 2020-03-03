package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
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

	private static DynamodbTestUtils dynamodbTestUtils;

	@BeforeAll
	static void beforeAll() throws Exception {
		dynamodbTestUtils = new DynamodbTestUtils(LOCAL_DYNAMO, NETWORK);
		dynamodbTestUtils.createTable("JB_Books", "isbn");
	}

	@AfterAll
	static void afterAll() {
		NETWORK.close();
	}

	/**
	 * Test for {@link DynamodbTestUtils#importData(File)}.
	 */
	@Test
	void testImportData() throws IOException, InterruptedException {
		final ClassLoader classLoader = DynamodbTestUtilsTestIT.class.getClassLoader();
		final File books = new File(classLoader.getResource("books.json").getFile());
		dynamodbTestUtils.importData(books);
		assertThat(dynamodbTestUtils.scan("JB_Books"), equalTo(3));
	}
}
