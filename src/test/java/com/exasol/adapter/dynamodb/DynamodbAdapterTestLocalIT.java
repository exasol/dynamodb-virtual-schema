package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.amazonaws.services.dynamodbv2.model.*;
import com.exasol.adapter.dynamodb.mapping.MappingTestFiles;
import com.exasol.adapter.dynamodb.mapping.TestDocuments;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;

/**
 * Tests the {@link DynamodbAdapter} using a local docker version of DynamoDB and a local docker version of exasol.
 **/
@Tag("integration")
@Testcontainers
class DynamodbAdapterTestLocalIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbAdapterTestLocalIT.class);

    private static final Network NETWORK = Network.newNetwork();
    @Container
    public static final GenericContainer LOCAL_DYNAMO = new GenericContainer<>("amazon/dynamodb-local")
            .withExposedPorts(8000).withNetwork(NETWORK).withNetworkAliases("dynamo")
            .withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");
    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> EXASOL_CONTAINER = new ExasolContainer<>()
            .withNetwork(NETWORK).withExposedPorts(8888).withLogConsumer(new Slf4jLogConsumer(LOGGER));
    private static final String TEST_SCHEMA = "TEST";
    private static final String DYNAMODB_CONNECTION = "DYNAMODB_CONNECTION";
    private static final String DYNAMO_BOOKS_TABLE = "MY_BOOKS";
    private static DynamodbTestInterface dynamodbTestInterface;
    private static TestcontainerExasolTestInterface exasolTestInterface;

    /**
     * Create a Virtual Schema in the Exasol test container accessing the local DynamoDB.
     */
    @BeforeAll
    static void beforeAll() throws DynamodbTestInterface.NoNetworkFoundException, SQLException, InterruptedException,
            BucketAccessException, TimeoutException, IOException {
        dynamodbTestInterface = new DynamodbTestInterface(LOCAL_DYNAMO, NETWORK);
        exasolTestInterface = new TestcontainerExasolTestInterface(EXASOL_CONTAINER);
        exasolTestInterface.uploadDynamodbAdapterJar();
        exasolTestInterface.uploadMapping(MappingTestFiles.BASIC_MAPPING_FILE_NAME);
        exasolTestInterface.uploadMapping(MappingTestFiles.SINGLE_COLUMN_TO_TABLE_MAPPING_FILE_NAME);
        exasolTestInterface.uploadMapping(MappingTestFiles.DATA_TYPE_TEST_MAPPING_FILE_NAME);
        exasolTestInterface.createAdapterScript();
        LOGGER.info("created adapter script");
        exasolTestInterface.createConnection(DYNAMODB_CONNECTION, dynamodbTestInterface.getDynamoUrl(),
                dynamodbTestInterface.getDynamoUser(), dynamodbTestInterface.getDynamoPass());
        LOGGER.info("created connection");
    }

    @AfterAll
    static void afterAll() {
        NETWORK.close();
        EXASOL_CONTAINER.getDockerClient().stopContainerCmd(EXASOL_CONTAINER.getContainerId()).withTimeout(10).exec();
    }

    @AfterEach
    void after() throws SQLException {
        dynamodbTestInterface.deleteCreatedTables();
        exasolTestInterface.dropVirtualSchema(TEST_SCHEMA);
    }

    @Test
    void testSchemaDefinition() throws SQLException {
        createBasicMappingVirtualSchema();
        final Map<String, String> rowNames = exasolTestInterface.describeTable(TEST_SCHEMA, "BOOKS");
        assertThat(rowNames, equalTo(Map.of("ISBN", "VARCHAR(20) UTF8", "NAME", "VARCHAR(100) UTF8", "AUTHOR_NAME",
                "VARCHAR(20) UTF8", "PUBLISHER", "VARCHAR(100) UTF8", "PRICE", "VARCHAR(10) UTF8")));
    }

    /**
     * Helper function that runs a {@code SELECT *} and return a single string column. In addition the execution time is
     * measured.
     */
    private SelectStringArrayResult selectStringArray() throws SQLException {
        final long start = System.currentTimeMillis();
        final ResultSet actualResultSet = exasolTestInterface.getStatement()
                .executeQuery("SELECT \"ISBN\" FROM " + TEST_SCHEMA + ".\"BOOKS\";");
        final long duration = System.currentTimeMillis() - start;
        final List<String> result = new ArrayList<>();
        while (actualResultSet.next()) {
            result.add(actualResultSet.getString("ISBN"));
        }
        LOGGER.info("query execution time was: {}", duration);
        return new SelectStringArrayResult(result, duration);
    }

    /**
     * Tests an {@code SELECT *} from an empty DynamoDB table.
     */
    @Test
    void testEmptySelect() throws SQLException {
        createBasicMappingVirtualSchema();
        dynamodbTestInterface.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        final List<String> result = selectStringArray().rows;
        assertThat(result.size(), equalTo(0));
    }

    /**
     * Tests an {@code SELECT *} from an DynamoDB table with a single line.
     */
    @Test
    void testSingleLineSelect() throws SQLException {
        createBasicMappingVirtualSchema();
        dynamodbTestInterface.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        final String Isbn = "12398439493";
        dynamodbTestInterface.putItem(DYNAMO_BOOKS_TABLE, Isbn, "test name");
        final SelectStringArrayResult result = selectStringArray();
        assertThat(result.rows, containsInAnyOrder(Isbn));
    }

    /**
     * Tests an {@code SELECT *} from an DynamoDB table with a single line with string result.
     */
    @Test
    void testSingleLineSelectWithStringResult() throws SQLException {
        createBasicMappingVirtualSchema();
        dynamodbTestInterface.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        final String Isbn = "abc";
        dynamodbTestInterface.putItem(DYNAMO_BOOKS_TABLE, Isbn, "test name");
        final SelectStringArrayResult result = selectStringArray();
        assertThat(result.rows, containsInAnyOrder(Isbn));
    }

    /**
     * Tests a {@code SELECT *} from a DynamoDB table with multiple lines.
     */
    @Test
    void testMultiLineSelect() throws IOException, SQLException {
        createBasicMappingVirtualSchema();
        dynamodbTestInterface.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestInterface.importData(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS);
        final List<String> result = selectStringArray().rows;
        assertThat(result, containsInAnyOrder("123567", "123254545", "1235673"));
    }

    /**
     * Tests a {@code SELECT *} from a large DynamoDB table.
     */
    @Test
    void testBigScan() throws SQLException {
        createBasicMappingVirtualSchema();
        dynamodbTestInterface.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        final int numBooks = 1000;
        final List<String> actualBookNames = new ArrayList<>(numBooks);
        for (int i = 0; i < numBooks; i++) {
            final String booksName = String.valueOf(i);
            dynamodbTestInterface.putItem(DYNAMO_BOOKS_TABLE, booksName, "name equal for all books");
            actualBookNames.add(booksName);
        }
        final SelectStringArrayResult result = selectStringArray();
        assertThat(result.rows, containsInAnyOrder(actualBookNames.toArray()));
    }

    @Test
    void testSelectNestedTableSchema() throws SQLException, IOException {
        createNestedTableVirtualSchema();
        final Map<String, String> rowNames = exasolTestInterface.describeTable(TEST_SCHEMA, "BOOKS_TOPICS");
        assertThat(rowNames, equalTo(Map.of("NAME", "VARCHAR(254) UTF8", "BOOKS_ISBN", "VARCHAR(20) UTF8")));
    }

    @Test
    void testSelectNestedTableResult() throws SQLException, IOException {
        createNestedTableVirtualSchema();
        final ResultSet actualResultSet = exasolTestInterface.getStatement()
                .executeQuery("SELECT NAME FROM " + TEST_SCHEMA + ".\"BOOKS_TOPICS\";");
        final List<String> topics = new ArrayList<>();
        while (actualResultSet.next()) {
            topics.add(actualResultSet.getString("NAME"));
        }
        assertThat(topics, containsInAnyOrder("Exasol", "DynamoDB", "Virtual Schema", "Fantasy", "Birds", "Nature"));
    }

    // TODO change test to filter on name when bug is fixed: https://github.com/exasol/dynamodb-virtual-schema/issues/55
    @Test
    void testJoinOnNestedTable() throws IOException, SQLException {
        createNestedTableVirtualSchema();
        final ResultSet actualResultSet = exasolTestInterface.getStatement()
                .executeQuery("SELECT BOOKS_TOPICS.NAME as TOPIC FROM " + TEST_SCHEMA + ".BOOKS JOIN " + TEST_SCHEMA
                        + ".\"BOOKS_TOPICS\" ON ISBN = BOOKS_ISBN WHERE ISBN = '123567';");
        final List<String> topics = new ArrayList<>();
        while (actualResultSet.next()) {
            topics.add(actualResultSet.getString("TOPIC"));
        }
        assertThat(topics, containsInAnyOrder("Exasol", "DynamoDB", "Virtual Schema"));
    }

    @Test
    void testSelection() throws SQLException, IOException {
        final String selectedIsbn = "123567";
        createBasicMappingVirtualSchema();
        dynamodbTestInterface.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestInterface.importData(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS);
        final ResultSet actualResultSet = exasolTestInterface.getStatement()
                .executeQuery("SELECT ISBN FROM " + TEST_SCHEMA + ".\"BOOKS\" WHERE ISBN = '" + selectedIsbn + "';");
        final List<String> isbns = new ArrayList<>();
        while (actualResultSet.next()) {
            isbns.add(actualResultSet.getString("ISBN"));
        }
        assertThat(isbns, containsInAnyOrder(selectedIsbn));
    }

    @Test
    void testGreaterSelectionWithSortKey() throws SQLException, IOException {
        createBasicMappingVirtualSchema();
        createTableBooksTableWithPublisherPriceKey();
        final ResultSet actualResultSet = exasolTestInterface.getStatement().executeQuery(
                "SELECT ISBN FROM " + TEST_SCHEMA + ".\"BOOKS\" WHERE PUBLISHER = 'jb books' AND PRICE > 10;");
        final List<String> isbns = new ArrayList<>();
        while (actualResultSet.next()) {
            isbns.add(actualResultSet.getString("ISBN"));
        }
        assertThat(isbns, containsInAnyOrder("123567"));
    }

    @Test
    void testLessSelectionWithSortKey() throws SQLException, IOException {
        createBasicMappingVirtualSchema();
        createTableBooksTableWithPublisherPriceKey();
        final ResultSet actualResultSet = exasolTestInterface.getStatement().executeQuery(
                "SELECT ISBN FROM " + TEST_SCHEMA + ".\"BOOKS\" WHERE PUBLISHER = 'jb books' AND PRICE < 11;");
        final List<String> isbns = new ArrayList<>();
        while (actualResultSet.next()) {
            isbns.add(actualResultSet.getString("ISBN"));
        }
        assertThat(isbns, containsInAnyOrder("123254545"));
    }

    private void createTableBooksTableWithPublisherPriceKey() throws IOException {
        final CreateTableRequest request = new CreateTableRequest().withTableName(DYNAMO_BOOKS_TABLE)
                .withKeySchema(new KeySchemaElement("publisher", KeyType.HASH),
                        new KeySchemaElement("price", KeyType.RANGE))
                .withAttributeDefinitions(new AttributeDefinition("publisher", ScalarAttributeType.S),
                        new AttributeDefinition("price", ScalarAttributeType.N))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
        dynamodbTestInterface.createTable(request);
        dynamodbTestInterface.importData(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS);
    }

    @Test
    void testDataTypes() throws IOException, SQLException {
        createDataTypesVirtualSchema();
        final ResultSet actualResultSet = exasolTestInterface.getStatement().executeQuery("SELECT * FROM " + TEST_SCHEMA
                + "." + MappingTestFiles.DATA_TYPE_TEST_EXASOL_TABLE_NAME + " WHERE STRINGVALUE = 'test';");
        actualResultSet.next();
        assertAll(() -> assertThat(actualResultSet.getString("STRINGVALUE"), equalTo("test")),
                () -> assertThat(actualResultSet.getString("BOOLVALUE"), equalTo("true")),
                () -> assertThat(actualResultSet.getString("DOUBLEVALUE"), equalTo("1.2")),
                () -> assertThat(actualResultSet.getString("INTEGERVALUE"), equalTo("1")),
                () -> assertThat(actualResultSet.getString("NULLVALUE"), equalTo(null))//
        );
    }

    private void createNestedTableVirtualSchema() throws SQLException, IOException {
        exasolTestInterface.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                "/bfsdefault/default/mappings/" + MappingTestFiles.SINGLE_COLUMN_TO_TABLE_MAPPING_FILE_NAME);
        dynamodbTestInterface.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestInterface.importData(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS);
    }

    void createBasicMappingVirtualSchema() throws SQLException {
        exasolTestInterface.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                "/bfsdefault/default/mappings/" + MappingTestFiles.BASIC_MAPPING_FILE_NAME);
    }

    void createDataTypesVirtualSchema() throws SQLException, IOException {
        exasolTestInterface.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                "/bfsdefault/default/mappings/" + MappingTestFiles.DATA_TYPE_TEST_MAPPING_FILE_NAME);
        dynamodbTestInterface.createTable(MappingTestFiles.DATA_TYPE_TEST_SRC_TABLE_NAME,
                TestDocuments.DATA_TYPE_TEST_STRING_VALUE);
        dynamodbTestInterface.importData(MappingTestFiles.DATA_TYPE_TEST_SRC_TABLE_NAME, TestDocuments.DATA_TYPE_TEST);
    }

    private static final class SelectStringArrayResult {
        public final List<String> rows;
        public final long duration;

        public SelectStringArrayResult(final List<String> rows, final long duration) {
            this.rows = rows;
            this.duration = duration;
        }
    }
}
