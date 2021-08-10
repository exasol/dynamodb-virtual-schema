package com.exasol.adapter.document;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.*;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;
import javax.json.JsonValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exasol.ExaConnectionInformation;
import com.exasol.dynamodb.DynamodbConnectionFactory;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Using is the abstract basis for DynamoDB test interfaces. The test interfaces offers convenience methods for creating
 * test setups for a DynamoDB.
 */
public abstract class DynamodbTestDbBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbTestDbBuilder.class);
    private final DynamoDbClient dynamoClient;
    private final String dynamoUrl;
    private final String dynamoUser;
    private final String dynamoPass;
    private final Optional<String> sessionToken;
    private final List<String> tableNames = new LinkedList<>();

    /**
     * Constructor called by all other constructors.
     */
    protected DynamodbTestDbBuilder(final String dynamoUrl, final String user, final String pass,
            final Optional<String> sessionToken) throws URISyntaxException {
        this.dynamoUrl = dynamoUrl;
        this.dynamoUser = user;
        this.dynamoPass = pass;
        this.sessionToken = sessionToken;
        this.dynamoClient = new DynamodbConnectionFactory().getConnection(dynamoUrl, user, pass, sessionToken);
    }

    public DynamoDbClient getDynamodbLowLevelConnection() throws URISyntaxException {
        return new DynamodbConnectionFactory().getConnection(this.getDynamoUrl(), this.getDynamoUser(),
                this.getDynamoPass(), this.sessionToken);
    }

    /**
     * Puts an item to a given table.
     */
    public void putItem(final String tableName, final String isbn, final String name) {
        this.dynamoClient.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of("isbn",
                AttributeValueQuickCreator.forString(isbn), "name", AttributeValueQuickCreator.forString(name)))
                .build());
    }

    /**
     * Add one ore more items to a given table defined by a JSON string.
     * 
     * @param tableName name of the table to put the items in
     * @param itemsJson json definitions of the items
     */
    public void putJson(final String tableName, final String... itemsJson) {
        final DynamodbBatchWriter dynamodbBatchWriter = new DynamodbBatchWriter(this.dynamoClient, tableName);
        Arrays.asList(itemsJson).forEach(dynamodbBatchWriter);
        dynamodbBatchWriter.flush();
    }

    /**
     * Runs a table scan on the given DynamoDB table. The scan result is logged.
     * 
     * @param tableName DynamoDB table name to scan
     * @return number of scanned items
     */
    public int scan(final String tableName) {
        final ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();
        return this.dynamoClient.scanPaginator(scanRequest).stream().reduce(0,
                (subtotal, response) -> subtotal + response.count(), Integer::sum);
    }

    /**
     * Create a DynamoDB table.
     * 
     * @param tableName name for the new DynamoDB table
     * @param keyName   partition key (type is always string)
     */
    public void createTable(final String tableName, final String keyName) {
        final CreateTableRequest.Builder createTableRequestBuilder = CreateTableRequest.builder();
        createTableRequestBuilder.tableName(tableName);
        createTableRequestBuilder
                .keySchema(List.of(KeySchemaElement.builder().attributeName(keyName).keyType(KeyType.HASH).build()));
        createTableRequestBuilder.attributeDefinitions(List
                .of(AttributeDefinition.builder().attributeName(keyName).attributeType(ScalarAttributeType.S).build()));
        createTableRequestBuilder.provisionedThroughput(
                ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build());
        this.createTable(createTableRequestBuilder.build());
    }

    /**
     * Create a DynamoDB table.
     * 
     * @param request {@link CreateTableRequest}
     */
    public void createTable(final CreateTableRequest request) {
        this.dynamoClient.createTable(request);
        this.tableNames.add(request.tableName());
    }

    /**
     * Deletes a DynamoDB table.
     * 
     * @param tableName name of the DynamoDB table to delete
     */
    public void deleteTable(final String tableName) {
        final DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder().tableName(tableName).build();
        this.dynamoClient.deleteTable(deleteTableRequest);
        this.tableNames.removeIf(table -> table.equals(tableName));
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
     * Imports data from a json file.
     *
     * @param tableName name of the DynamoDB table
     * @param asset     JSON file to import
     * @throws IOException if file can't get opened
     */
    public void importData(final String tableName, final File asset) throws IOException {
        try (final JsonReader jsonReader = Json.createReader(new FileReader(asset))) {
            final String[] itemsJson = splitJsonArrayInArrayOfJsonStrings(jsonReader.readArray());
            putJson(tableName, itemsJson);
        }
    }

    public void importData(final String tableName, final InputStream stream) throws IOException {
        try (final JsonReader jsonReader = Json.createReader(stream)) {
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

    public Optional<String> getSessionToken() {
        return this.sessionToken;
    }

    public ExaConnectionInformation getExaConnectionInformationForDynamodb() {
        return new ExaConnectionInformation() {
            @Override
            public ConnectionType getType() {
                return ConnectionType.PASSWORD;
            }

            @Override
            public String getAddress() {
                return DynamodbTestDbBuilder.this.getDynamoUrl();
            }

            @Override
            public String getUser() {
                return DynamodbTestDbBuilder.this.getDynamoUser();
            }

            @Override
            public String getPassword() {
                return DynamodbTestDbBuilder.this.getDynamoPass();
            }
        };
    }

    public void dropAllTables() {
        final ListTablesResponse listTablesResponse = this.dynamoClient.listTables();
        listTablesResponse.tableNames().forEach(this::deleteTable);
    }

    @SuppressWarnings("serial")
    public static class NoNetworkFoundException extends Exception {
        public NoNetworkFoundException() {
            super("no matching network was found");
        }
    }
}
