package com.exasol.adapter.dynamodb;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonArray;
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
import com.exasol.dynamodb.DynamodbConnectionFactory;
import com.github.dockerjava.api.model.ContainerNetwork;

/*
    Using this class test data can be put to DynamoDB.
 */
public class DynamodbTestInterface {
    // Default credentials for dynamodb docker
    private static final String LOCAL_DYNAMO_USER = "fakeMyKeyId";
    private static final String LOCAL_DYNAMO_PASS = "fakeSecretAccessKey";
    private static final String LOCAL_DYNAMO_PORT = "8000";
    private static final String AWS_LOCAL_URL = "aws:eu-central-1";
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbTestInterface.class);

    private final DynamoDB dynamoClient;
    private final String dynamoUrl;
    private final String dynamoUser;
    private final String dynamoPass;
    private final List<String> tableNames = new LinkedList<>();

    /**
     * Constructor for DynamoDB at AWS with credentials from system AWS configuration.
     * <p>
     * Use {@code aws configure} to set up.
     * </p>
     */
    public DynamodbTestInterface() {
        this(DefaultAWSCredentialsProviderChain.getInstance().getCredentials());
    }

    /**
     * Constructor using DynamoDB at AWS with given AWS credentials.
     */
    private DynamodbTestInterface(final AWSCredentials awsCredentials) {
        this(awsCredentials.getAWSAccessKeyId(), awsCredentials.getAWSSecretKey());
    }

    /**
     * Constructor using DynamoDB at AWS with given user and pass.
     */
    private DynamodbTestInterface(final String user, final String pass) {
        this(AWS_LOCAL_URL, user, pass);
    }

    /**
     * Constructor using default login credentials for the local dynamodb docker instance.
     */
    public DynamodbTestInterface(final GenericContainer localDynamo, final Network dockerNetwork)
            throws NoNetworkFoundException {
        this(getDockerNetworkUrlForLocalDynamodb(localDynamo, dockerNetwork), LOCAL_DYNAMO_USER, LOCAL_DYNAMO_PASS);
    }

    /**
     * Constructor called by all other constructors.
     */
    private DynamodbTestInterface(final String dynamoUrl, final String user, final String pass) {
        this.dynamoUrl = dynamoUrl;
        this.dynamoUser = user;
        this.dynamoPass = pass;
        this.dynamoClient = new DynamodbConnectionFactory().getDocumentConnection(dynamoUrl, user, pass);
    }

    private static String getDockerNetworkUrlForLocalDynamodb(final GenericContainer localDynamo,
            final Network thisNetwork) throws NoNetworkFoundException {
        final Map<String, ContainerNetwork> networks = localDynamo.getContainerInfo().getNetworkSettings()
                .getNetworks();
        for (final ContainerNetwork network : networks.values()) {
            if (thisNetwork.getId().equals(network.getNetworkID())) {
                return "http://" + network.getIpAddress() + ":" + LOCAL_DYNAMO_PORT;
            }
        }
        throw new NoNetworkFoundException();
    }

    /**
     * Puts an item to a given table.
     */
    public void putItem(final String tableName, final String isbn, final String name) {
        final Table table = this.dynamoClient.getTable(tableName);
        table.putItem(new Item().withPrimaryKey("isbn", isbn).withString("name", name));
    }

    /**
     * Adds one ore more items to a given table defined by a JSON string.
     * 
     * @param tableName name of the table to put the items in
     * @param itemsJson json definitions of the items
     */
    public void putJson(final String tableName, final String... itemsJson) {
        final TableWriteItems writeRequest = new TableWriteItems(tableName)
                .withItemsToPut(Arrays.stream(itemsJson).map(Item::fromJSON).toArray(Item[]::new));
        this.dynamoClient.batchWriteItem(writeRequest);
    }

    /**
     * Runs a table scan on the given DynamoDB table. The scan result is logged.
     * 
     * @param tableName DynamoDB table name to scan
     * @return number of scanned items
     */
    public int scan(final String tableName) {
        final Table table = this.dynamoClient.getTable(tableName);
        final ItemCollection<ScanOutcome> scanResult = table.scan();
        return logAndCountItems(scanResult);
    }

    private int logAndCountItems(final Iterable<Item> items) {
        int counter = 0;
        for (final Item item : items) {
            LOGGER.trace("scanned item: {}", item);
            counter++;
        }
        return counter;
    }

    /**
     * Creates a DynamoDB table.
     * 
     * @param tableName name for the new DynamoDB table
     * @param keyName   partition key (type is always string)
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
     * @param tableName name of the DynamoDB table to delete
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
            deleteTable(tableName);
        }
    }

    /**
     * Imports data from a json file. The File mus have the AWS json syntax. For running the import the aws-cli is used.
     *
     * @param tableName name of the DynamoDB table
     * @param asset     JSOn file to import
     * @throws IOException if file can't get opened
     */
    public void importData(final String tableName, final File asset) throws IOException {
        try (final JsonReader jsonReader = Json.createReader(new FileReader(asset))) {
            final String[] itemsJson = splitJsonArrayInArrayOfJsonStrings(jsonReader.readArray());
            putJson(tableName, itemsJson);
        }
    }

    private String[] splitJsonArrayInArrayOfJsonStrings(final JsonArray jsonArray) {
        return jsonArray.stream()//
                .map(JsonValue::toString)//
                .toArray(String[]::new);
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

    @SuppressWarnings("serial")
    public static class NoNetworkFoundException extends Exception {
        public NoNetworkFoundException() {
            super("no matching network was found");
        }
    }
}