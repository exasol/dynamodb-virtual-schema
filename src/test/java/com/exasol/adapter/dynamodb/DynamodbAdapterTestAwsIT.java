package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

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

/**
 * Tests using the AWS DynamoDB. Setup credentials on your machine using:
 * {@code aws configure} For now two factor authentication is NOT SUPPORTED!
 * 
 * Preparation: create a table {@code JB_Books} with primary key {@code isbn} and insert one
 * item
 */
@Testcontainers
public class DynamodbAdapterTestAwsIT {
	private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbAdapterTestLocalIT.class);

	@Container
	private static final ExasolContainer<? extends ExasolContainer<?>> exasolContainer = new ExasolContainer<>()
			.withLogConsumer(new Slf4jLogConsumer(LOGGER));
	private static DynamodbTestUtils dynamodbTestUtils;
	private static ExasolTestUtils exasolTestUtils;
	private static final String TEST_SCHEMA = "TEST";
	private static final String DYNAMODB_CONNECTION = "DYNAMODB_CONNECTION";

	/**
	 * Creates a Virtual Schema in the Exasol test container accessing DynamoDB on
	 * AWS
	 */
	@BeforeAll
	static void beforeAll()
			throws SQLException, BucketAccessException, InterruptedException, java.util.concurrent.TimeoutException {
		dynamodbTestUtils = new DynamodbTestUtils();
		exasolTestUtils = new ExasolTestUtils(exasolContainer);
		exasolTestUtils.uploadDynamodbAdapterJar();
		exasolTestUtils.createAdapterScript();
		exasolTestUtils.createConnection(DYNAMODB_CONNECTION, dynamodbTestUtils.getDynamoUrl(),
				dynamodbTestUtils.getDynamoUser(), dynamodbTestUtils.getDynamoPass());
		exasolTestUtils.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION);
	}

	/**
	 * Tests a simple {@code SELECT}
	 */
	@Test
	void testSelect() throws SQLException {
		final ResultSet result = exasolTestUtils.getStatement()
				.executeQuery("SELECT * FROM " + TEST_SCHEMA + ".\"testTable\";");// table name is hardcoded in adapter
																					// definition (DynamodbAdapter)
		assertThat(result, notNullValue());
		assertThat(result.next(), is(true));
		assertThat(result.getString(1), equalTo("1234234243"));
		assertThat(result.next(), is(false));
	}
}
