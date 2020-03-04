package com.exasol.adapter.dynamodb;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.*;
import com.github.dockerjava.api.model.ContainerNetwork;

/*
    Test utils for testing with DynamoDB
 */
public class DynamodbTestUtils {
	// Default credentials for dynamodb docker
	private static final String LOCAL_DYNAMO_USER = "fakeMyKeyId";
	private static final String LOCAL_DYNAMO_PASS = "fakeSecretAccessKey";
	private static final String LOCAL_DYNAMO_PORT = "8000";
	private static final String AWS_LOCAL_URL = "aws:eu-central-1";
	private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbAdapterTestLocalIT.class);

	private final DynamoDB dynamoClient;
	private final String dynamoUrl;
	private final String dynamoUser;
	private final String dynamoPass;
	private final List<String> tableNames = new LinkedList<>();

	/**
	 * Constructor for DynamoDB at AWS with credentials from system AWS
	 * configuration.
	 * <p>
	 * Use {@code aws configure} to set up.
	 * </p>
	 */
	public DynamodbTestUtils() {
		this(DefaultAWSCredentialsProviderChain.getInstance().getCredentials());
	}

	/**
	 * Constructor using DynamoDB at AWS with given AWS credentials.
	 */
	private DynamodbTestUtils(final AWSCredentials awsCredentials) {
		this(awsCredentials.getAWSAccessKeyId(), awsCredentials.getAWSSecretKey());
	}

	/**
	 * Constructor using DynamoDB at AWS with given user and pass.
	 */
	private DynamodbTestUtils(final String user, final String pass) {
		this(AWS_LOCAL_URL, user, pass);
	}

	/**
	 * Constructor using default login credentials for the local dynamodb docker
	 * instance.
	 */
	public DynamodbTestUtils(final GenericContainer localDynamo, final Network dockerNetwork) throws Exception {
		this(getDockerNetworkUrlForLocalDynamodb(localDynamo, dockerNetwork), LOCAL_DYNAMO_USER, LOCAL_DYNAMO_PASS);
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

	/**
	 * Constructor called by all other constructors.
	 */
	private DynamodbTestUtils(final String dynamoUrl, final String user, final String pass) {
		this.dynamoUrl = dynamoUrl;
		this.dynamoUser = user;
		this.dynamoPass = pass;
		this.dynamoClient = DynamodbAdapter.getDynamodbConnection(dynamoUrl, user, pass);
	}

	/**
	 * Adds a book to the table {@code JB_Books}.
	 * 
	 * @param isbn
	 * @param name
	 */
	public void putItem(final String tableName, final String isbn, final String name) {
		final Table table = this.dynamoClient.getTable(tableName);
		table.putItem(new Item().withPrimaryKey("isbn", isbn).withString("name", name));
	}

	public void putJson(final String tableName, final String... itemsJson) {
		final Table table = this.dynamoClient.getTable(tableName);
		for (final String json : itemsJson) {
			table.putItem(Item.fromJSON(json));
		}
	}

	/**
	 * Runs a table scan on the given DynamoDB table. The scan result is logged.
	 * 
	 * @param tableName
	 * @return number of scanned items
	 */
	public int scan(final String tableName) {
		final Table table = this.dynamoClient.getTable(tableName);
		final ItemCollection<ScanOutcome> scanResult = table.scan();
		for (final Item item : scanResult) {// it is important to scan all items here for getAccumulatedScannedCount to
											// work
			LOGGER.trace(item.toString());
		}
		return scanResult.getAccumulatedScannedCount();
	}

	/**
	 * Creates a DynamoDB table.
	 * 
	 * @param tableName
	 * @param keyName
	 *            partition key (type is always string)
	 */
	public void createTable(final String tableName, final String keyName) {
		this.dynamoClient.createTable(tableName, List.of(new KeySchemaElement(keyName, KeyType.HASH)), // key schema
				List.of(new AttributeDefinition(keyName, ScalarAttributeType.S)), // attribute definitions
				new ProvisionedThroughput(1L, 1L));
		this.tableNames.add(tableName);
	}

	/**
	 * Deletes a DynamoDB table.
	 * 
	 * @param tableName
	 */
	public void deleteTable(final String tableName) {
		this.dynamoClient.getTable(tableName).delete();
		this.tableNames.removeIf(n -> n.equals(tableName));
	}

	/**
	 * Deletes all tables created with {@link #createTable(String, String)}.
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
	public void importData(final String tableNames, final File asset) throws IOException {
		final JsonReader jsonReader = Json.createReader(new FileReader(asset));
		// split array of object into multiple json strings
		final String[] itemsJson = jsonReader.readArray().stream()//
				.map(JsonValue::toString)//
				.toArray(String[]::new);
		this.putJson(tableNames, itemsJson);
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
