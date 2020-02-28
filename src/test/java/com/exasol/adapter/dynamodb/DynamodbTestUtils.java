package com.exasol.adapter.dynamodb;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import com.github.dockerjava.api.model.ContainerNetwork;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/*
    Test utils for testing DynamoDB
 */
public class DynamodbTestUtils {
	// default credentials for dynamodb docker
	private static final String LOCAL_DYNAMO_USER = "fakeMyKeyId";
	private static final String LOCAL_DYNAMO_PASS = "fakeSecretAccessKey";
	private static final String LOCAL_DYNAMO_PORT = "8000";
	private static final String LOCALHOST_IP = "127.0.0.1";
	private static final String AWS_LOCAL_URL = "aws:eu-central-1";

	private final DynamoDbClient dynamoClient;
	private final String dynamoUrl;
	private final String localUrl;

	private final String dynamoUser;
	private final String dynamoPass;

	private final List<String> tableNames = new LinkedList<>();

	private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbAdapterTestLocalIT.class);

	/**
	 * Creates a DynamodbTestUtils for AWS with credentials from system AWS
	 * configuration.
	 * <p>
	 * Use {@code aws configure} to set up.
	 * </p>
	 */
	public DynamodbTestUtils() {
		this(DefaultCredentialsProvider.create().resolveCredentials());
	}

	/*
	 * Creates a DynamodbTestUtils for AWS with given credentials
	 */
	private DynamodbTestUtils(final AwsCredentials awsCredentials) {
		this(awsCredentials.accessKeyId(), awsCredentials.secretAccessKey());
	}

	/*
	 * Create DynamodbTestUtils for AWS connection
	 */
	private DynamodbTestUtils(final String user, final String pass) {
		this(AWS_LOCAL_URL, AWS_LOCAL_URL, user, pass);
	}

	/*
	 * Creates an DynamodbTestUtils instance with default login credentials for the
	 * local dynamodb docker instance
	 */
	public DynamodbTestUtils(final GenericContainer localDynamo, final Network dockerNetwork) throws Exception {
		this(getLocalUrlForLocalDynamodb(localDynamo), getDockerNetworkUrlForLocalDynamodb(localDynamo, dockerNetwork),
				LOCAL_DYNAMO_USER, LOCAL_DYNAMO_PASS);
	}

	private static String getDockerNetworkUrlForLocalDynamodb(final GenericContainer localDynamo,
			final Network thisNetwork) throws Exception {
		final Map<String, ContainerNetwork> networks = localDynamo.getContainerInfo().getNetworkSettings()
				.getNetworks();
		for (final ContainerNetwork network : networks.values()) {
			if (thisNetwork.getId().equals(network.getNetworkID())) {
				return "http://" + network.getIpAddress() + ":" + LOCAL_DYNAMO_PORT;
			}
		}
		throw new Exception("no network found");
	}

	private static String getLocalUrlForLocalDynamodb(final GenericContainer localDynamo) {
		return "http://" + LOCALHOST_IP + ":" + localDynamo.getFirstMappedPort();
	}

	/**
	 * Constructor called by all other constructors
	 */
	private DynamodbTestUtils(final String localUrl, final String dynamoUrl, final String user, final String pass) {
		this.localUrl = localUrl;
		this.dynamoUrl = dynamoUrl;
		this.dynamoUser = user;
		this.dynamoPass = pass;
		this.dynamoClient = DynamodbAdapter.getDynamodbConnection(dynamoUrl, user, pass);
	}

	/**
	 * Adds a book to the table JB_Books
	 * 
	 * @param isbn
	 * @param name
	 */
	public void pushBook(final String isbn, final String name) {
		final HashMap<String, AttributeValue> itemValues = new HashMap<>();
		itemValues.put("isbn", AttributeValue.builder().s(isbn).build());
		itemValues.put("name", AttributeValue.builder().s(name).build());
		final PutItemRequest request = PutItemRequest.builder().tableName("JB_Books").item(itemValues).build();
		this.dynamoClient.putItem(request);
	}

	/**
	 * Runs a table scan on the given DynamoDB table. The scan result is logged.
	 * 
	 * @param tableName
	 * @return number of scanned items
	 */
	public int scan(final String tableName) {
		final ScanResponse result = this.dynamoClient.scan(ScanRequest.builder().tableName(tableName).build());
		LOGGER.trace(result.toString());
		return result.count();
	}

	/**
	 * Creates a DynamoDB table
	 * 
	 * @param tableName
	 * @param keyName
	 *            partition key (type is always string)
	 */
	public void createTable(final String tableName, final String keyName) {
		final CreateTableRequest request = CreateTableRequest.builder().tableName(tableName)
				.attributeDefinitions(AttributeDefinition.builder().attributeName(keyName)
						.attributeType(ScalarAttributeType.S).build())
				.keySchema(KeySchemaElement.builder().keyType(KeyType.HASH).attributeName(keyName).build())
				.provisionedThroughput(
						ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build())
				.build();
		this.dynamoClient.createTable(request);
		this.tableNames.add(tableName);
	}

	/**
	 * deletes a DynamoDB table
	 * 
	 * @param tableName
	 */
	public void deleteTable(final String tableName) {
		final DeleteTableRequest deleteRequest = DeleteTableRequest.builder().tableName(tableName).build();
		this.dynamoClient.deleteTable(deleteRequest);
		this.tableNames.removeIf(n -> n.equals(tableName));
	}

	/**
	 * deletes all tables created with {@link #createTable(String, String)}
	 */
	public void deleteCreatedTables() {
		for (final String tableName : this.tableNames) {
			this.deleteTable(tableName);
		}
	}

	/**
	 * Imports data from a json file. The File mus have the AWS json syntax. For
	 * running the import the aws-cli is used.
	 * 
	 * @param asset
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void importData(final File asset) throws IOException, InterruptedException {
		final Runtime runtime = Runtime.getRuntime();
		String importCommand = "aws dynamodb batch-write-item --request-items file://" + asset.getPath();
		if (!this.localUrl.equals(AWS_LOCAL_URL)) {
			importCommand += " --endpoint-url " + this.localUrl;
		}
		LOGGER.trace(importCommand);
		final Process process = runtime.exec(importCommand);
		final InputStream stderr = process.getErrorStream();
		final InputStreamReader inputStreamReader = new InputStreamReader(stderr);
		final BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			LOGGER.error(line);
		}
		process.waitFor();
	}

	public String getDynamoUrl() {
		return this.dynamoUrl;
	}

	public String getDynamoUser() {
		return this.dynamoUser;
	}

	public String getDynamoPass() {
		return this.dynamoPass;
	}
}
