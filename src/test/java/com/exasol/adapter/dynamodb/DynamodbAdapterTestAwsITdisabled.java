package com.exasol.adapter.dynamodb;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolContainerConstants;
import com.exasol.jdbc.TimeoutException;

import util.DynamodbTestUtils;
import util.ExasolTestUtils;
/*
  Tests using the aws DynamoDB.
  Setup credentials on your machine using: aws configure
  Until no two factor authentication is NOT SUPPORTED!

  Preparation:
  create a table JB_Books with primary key "isbn" and insert one item
 */
@Testcontainers
public class DynamodbAdapterTestAwsITdisabled {
	private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbAdapterTestLocalIT.class);

	@Container
	private static final ExasolContainer<? extends ExasolContainer<?>> exasolContainer = new ExasolContainer<>(
			ExasolContainerConstants.EXASOL_DOCKER_IMAGE_REFERENCE).withLogConsumer(new Slf4jLogConsumer(LOGGER));

	private static DynamodbTestUtils dynamodbTestUtils;
	private static ExasolTestUtils exasolTestUtils;

	private static final String TEST_SCHEMA = "TEST";
	private static final String DYNAMODB_CONNECTION = "DYNAMODB_CONNECTION";

	@BeforeAll
	static void beforeAll() throws SQLException, BucketAccessException, InterruptedException, TimeoutException,
			java.util.concurrent.TimeoutException {
		dynamodbTestUtils = new DynamodbTestUtils();
		exasolTestUtils = new ExasolTestUtils(exasolContainer);
		exasolTestUtils.uploadDynamodbAdapterJar();
		exasolTestUtils.createAdapterScript();
		exasolTestUtils.createConnection(DYNAMODB_CONNECTION, dynamodbTestUtils.getDynamoUrl(),
				dynamodbTestUtils.getDynamoUser(), dynamodbTestUtils.getDynamoPass());
		exasolTestUtils.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION);
	}

	@Test
	void testSelect() throws SQLException {
		final ResultSet expected = exasolTestUtils.getStatement()
				.executeQuery("SELECT * FROM " + TEST_SCHEMA + ".\"testTable\";");// table name is hardcoded in adapter
																					// definition (DynamodbAdapter)
		assertNotNull(expected);
		assertTrue(expected.next());
		assertEquals("1234234243", expected.getString(1));
		assertFalse(expected.next());
	}
}
