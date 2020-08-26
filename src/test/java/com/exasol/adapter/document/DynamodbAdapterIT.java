package com.exasol.adapter.document;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
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

import com.exasol.adapter.document.dynamodb.DynamodbAdapter;
import com.exasol.adapter.document.mapping.MappingTestFiles;
import com.exasol.adapter.document.mapping.TestDocuments;
import com.exasol.bucketfs.BucketAccessException;

import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Tests the {@link DynamodbAdapter} using a local docker version of DynamoDB and a local docker version of exasol.
 **/
@Tag("integration")
class DynamodbAdapterIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbAdapterIT.class);

    private static final String TEST_SCHEMA = "TEST";
    private static final String DYNAMODB_CONNECTION = "DYNAMODB_CONNECTION";
    private static final String DYNAMO_BOOKS_TABLE = "MY_BOOKS";
    public static final String BUCKETFS_PATH = "/bfsdefault/default/mappings/";
    private static ExasolTestInterface exasolTestInterface;
    private static DynamodbTestInterface dynamodbTestInterface;
    private static ExasolTestDatabaseBuilder exasolTestDatabaseBuilder;

    /**
     * Create a Virtual Schema in the Exasol test container accessing the local DynamoDB.
     */
    @BeforeAll
    static void beforeAll() throws DynamodbTestInterface.NoNetworkFoundException, SQLException, InterruptedException,
            BucketAccessException, TimeoutException, IOException, NoSuchAlgorithmException, KeyManagementException,
            URISyntaxException {
        final IntegrationTestSetup integrationTestSetup = new IntegrationTestSetup();
        exasolTestInterface = integrationTestSetup.getExasolTestInterface();
        exasolTestDatabaseBuilder = new ExasolTestDatabaseBuilder(exasolTestInterface);
        dynamodbTestInterface = integrationTestSetup.getDynamodbTestInterface();

        exasolTestDatabaseBuilder.uploadDynamodbAdapterJar();
        exasolTestDatabaseBuilder.uploadMappingTestFile(MappingTestFiles.BASIC_MAPPING,
                "mappings/" + MappingTestFiles.BASIC_MAPPING);
        exasolTestDatabaseBuilder.uploadMappingTestFile(MappingTestFiles.SINGLE_COLUMN_TO_TABLE_MAPPING,
                "mappings/" + MappingTestFiles.SINGLE_COLUMN_TO_TABLE_MAPPING);
        exasolTestDatabaseBuilder.uploadMappingTestFile(MappingTestFiles.DATA_TYPE_TEST_MAPPING,
                "mappings/" + MappingTestFiles.DATA_TYPE_TEST_MAPPING);
        exasolTestDatabaseBuilder.uploadMappingTestFile(MappingTestFiles.DOUBLE_NESTED_TO_TABLE_MAPPING,
                "mappings/" + MappingTestFiles.DOUBLE_NESTED_TO_TABLE_MAPPING);
        exasolTestDatabaseBuilder.uploadMappingTestFile(MappingTestFiles.TO_JSON_MAPPING,
                "mappings/" + MappingTestFiles.TO_JSON_MAPPING);
        Thread.sleep(3000); // Wait for BucketFS to sync
        exasolTestDatabaseBuilder.createAdapterScript();
        exasolTestDatabaseBuilder.createUdf();
        LOGGER.info("created adapter script");
        exasolTestDatabaseBuilder.createConnection(DYNAMODB_CONNECTION, dynamodbTestInterface.getDynamoUrl(),
                dynamodbTestInterface.getDynamoUser(), dynamodbTestInterface.getDynamoPass());
        LOGGER.info("created connection");
    }

    @AfterAll
    static void afterAll() {
        exasolTestInterface.teardown();
        dynamodbTestInterface.teardown();
    }

    @AfterEach
    void after() throws SQLException {
        dynamodbTestInterface.deleteCreatedTables();
        exasolTestDatabaseBuilder.dropVirtualSchema(TEST_SCHEMA);
    }

    @Test
    void testSchemaDefinition() throws SQLException {
        createBasicMappingVirtualSchema();
        final Map<String, String> rowNames = exasolTestDatabaseBuilder.describeTable(TEST_SCHEMA, "BOOKS");
        assertThat(rowNames, equalTo(Map.of("ISBN", "VARCHAR(20) UTF8", "NAME", "VARCHAR(100) UTF8", "AUTHOR_NAME",
                "VARCHAR(20) UTF8", "PUBLISHER", "VARCHAR(100) UTF8", "PRICE", "DECIMAL(8,2)")));
    }

    @Test
    void testToDecimalMapping() throws SQLException, IOException {
        createBasicMappingVirtualSchema();
        dynamodbTestInterface.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestInterface.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
        final String query = "SELECT PRICE FROM " + TEST_SCHEMA + ".BOOKS;";
        final ResultSet actualResultSet = exasolTestDatabaseBuilder.getStatement().executeQuery(query);
        final List<Double> result = new ArrayList<>();
        while (actualResultSet.next()) {
            result.add(actualResultSet.getDouble("PRICE"));
        }
        assertThat(result, containsInAnyOrder(10.0, 15.0, 21.12));
    }

    /**
     * Helper function that runs a {@code SELECT *} and return a single string column. In addition the execution time is
     * measured.
     */
    private SelectStringArrayResult selectStringArray() throws SQLException {
        final long start = System.currentTimeMillis();
        final ResultSet actualResultSet = exasolTestDatabaseBuilder.getStatement()
                .executeQuery("SELECT * FROM " + TEST_SCHEMA + ".\"BOOKS\";");
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
        dynamodbTestInterface.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
        final List<String> result = selectStringArray().rows;
        assertThat(result, containsInAnyOrder("123567", "123254545", "1235673"));
    }

    @Test
    void testAnyColumnProjection() throws SQLException, IOException {
        createBasicMappingVirtualSchema();
        dynamodbTestInterface.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestInterface.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
        final ResultSet resultSet = exasolTestDatabaseBuilder.getStatement()
                .executeQuery("SELECT COUNT(*) as NUMBER_OF_BOOKS FROM BOOKS;");
        resultSet.next();
        final int number_of_books = resultSet.getInt("NUMBER_OF_BOOKS");
        assertThat(number_of_books, equalTo(3));
    }

    @Test
    void testSelectNestedTableSchema() throws SQLException, IOException {
        createNestedTableVirtualSchema();
        final Map<String, String> rowNames = exasolTestDatabaseBuilder.describeTable(TEST_SCHEMA, "BOOKS_TOPICS");
        assertThat(rowNames, equalTo(Map.of("NAME", "VARCHAR(254) UTF8", "BOOKS_ISBN", "VARCHAR(20) UTF8")));
    }

    @Test
    void testSelectNestedTableResult() throws SQLException, IOException {
        createNestedTableVirtualSchema();
        final ResultSet actualResultSet = exasolTestDatabaseBuilder.getStatement()
                .executeQuery("SELECT NAME FROM " + TEST_SCHEMA + ".\"BOOKS_TOPICS\";");
        final List<String> topics = new ArrayList<>();
        while (actualResultSet.next()) {
            topics.add(actualResultSet.getString("NAME"));
        }
        assertThat(topics, containsInAnyOrder("Exasol", "DynamoDB", "Virtual Schema", "Fantasy", "Birds", "Nature"));
    }

    @Test
    void testJoinOnNestedTable() throws IOException, SQLException {
        createNestedTableVirtualSchema();
        final List<String> topics = runQueryAndExtractColumn(
                "SELECT BOOKS_TOPICS.NAME as TOPIC FROM " + TEST_SCHEMA + ".BOOKS JOIN " + TEST_SCHEMA
                        + ".\"BOOKS_TOPICS\" ON ISBN = BOOKS_ISBN WHERE BOOKS.NAME = 'bad book 1';",
                "TOPIC");
        assertThat(topics, containsInAnyOrder("Exasol", "DynamoDB", "Virtual Schema"));
    }

    @Test
    void testSelectionOnNestedTable() throws IOException, SQLException {
        createNestedTableVirtualSchema();
        final List<String> topics = runQueryAndExtractColumn(
                "SELECT BOOKS_TOPICS.NAME as TOPIC FROM " + TEST_SCHEMA
                        + ".\"BOOKS_TOPICS\" WHERE NAME =  'Exasol';",
                "TOPIC");
        assertThat(topics, containsInAnyOrder("Exasol"));
    }

    @Test
    void testJoinOnDoubleNestedTable() throws IOException, SQLException {
        createDoubleNestedTableVirtualSchema();
        final List<String> figures = runQueryAndExtractColumn("SELECT BOOKS_CHAPTERS_FIGURES.NAME as FIGURE FROM "
                + TEST_SCHEMA + ".BOOKS JOIN " + TEST_SCHEMA
                + ".\"BOOKS_CHAPTERS\" ON ISBN = BOOKS_CHAPTERS.BOOKS_ISBN " + "JOIN " + TEST_SCHEMA
                + ".BOOKS_CHAPTERS_FIGURES ON BOOKS_CHAPTERS.INDEX = BOOKS_CHAPTERS_INDEX AND ISBN = BOOKS_CHAPTERS_FIGURES.BOOKS_ISBN "
                + "WHERE BOOKS.NAME = 'bad book 1';", "FIGURE");
        assertThat(figures, containsInAnyOrder("Image of the Author", "figure 2", "figure 3"));
    }

    @Test
    void testSelectOnIndexColumn() throws SQLException, IOException {
        createDoubleNestedTableVirtualSchema();
        final List<String> figures = runQueryAndExtractColumn(
                "SELECT NAME FROM " + TEST_SCHEMA + ".BOOKS_CHAPTERS " + "WHERE \"INDEX\" = 0;", "NAME");
        assertThat(figures, containsInAnyOrder("Main Chapter", "chapter 1"));
    }

    @Test
    void testProjectionOnPropertyInList() throws SQLException, IOException {
        createDoubleNestedTableVirtualSchema();
        final List<String> figures = runQueryAndExtractColumn("SELECT NAME FROM " + TEST_SCHEMA + ".BOOKS_CHAPTERS",
                "NAME");
        assertThat(figures, containsInAnyOrder("Main Chapter", "chapter 1", "chapter 2"));
    }

    @Test
    void testSelectOnIndexAndOtherColumn() throws SQLException, IOException {
        createDoubleNestedTableVirtualSchema();
        final List<String> figures = runQueryAndExtractColumn("SELECT NAME FROM " + TEST_SCHEMA + ".BOOKS_CHAPTERS "
                + "WHERE \"INDEX\" = 0 AND NAME = 'Main Chapter';", "NAME");
        assertThat(figures, containsInAnyOrder("Main Chapter"));
    }

    @Test
    void testNestedTableWithCompoundForeignKey() throws IOException, SQLException {
        createDoubleNestedTableVirtualSchemaWithCompoundPrimaryKey();
        final Map<String, String> rowNames = exasolTestDatabaseBuilder.describeTable(TEST_SCHEMA, "BOOKS_CHAPTERS");
        assertThat(rowNames, equalTo(Map.of("NAME", "VARCHAR(254) UTF8", "BOOKS_ISBN", "VARCHAR(20) UTF8", "BOOKS_NAME",
                "VARCHAR(254) UTF8", "INDEX", "DECIMAL(9,0)")));
    }

    private List<String> runQueryAndExtractColumn(final String query, final String columnName) throws SQLException {
        final ResultSet actualResultSet = exasolTestDatabaseBuilder.getStatement().executeQuery(query);
        final List<String> topics = new ArrayList<>();
        while (actualResultSet.next()) {
            topics.add(actualResultSet.getString(columnName));
        }
        return topics;
    }

    @Test
    void testSelection() throws SQLException, IOException {
        final String selectedIsbn = "123567";
        createBasicMappingVirtualSchema();
        dynamodbTestInterface.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestInterface.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
        final ResultSet actualResultSet = exasolTestDatabaseBuilder.getStatement()
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
        final ResultSet actualResultSet = exasolTestDatabaseBuilder.getStatement().executeQuery(
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
        final ResultSet actualResultSet = exasolTestDatabaseBuilder.getStatement().executeQuery(
                "SELECT ISBN FROM " + TEST_SCHEMA + ".\"BOOKS\" WHERE PUBLISHER = 'jb books' AND PRICE < 11;");
        final List<String> isbns = new ArrayList<>();
        while (actualResultSet.next()) {
            isbns.add(actualResultSet.getString("ISBN"));
        }
        assertThat(isbns, containsInAnyOrder("123254545"));
    }

    @Test
    void testToJsonMapping() throws IOException, SQLException {
        createToJsonMappingVirtualSchema();
        dynamodbTestInterface.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestInterface.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
        final List<String> result = runQueryAndExtractColumn("SELECT TOPICS FROM " + TEST_SCHEMA + ".BOOKS", "TOPICS");
        assertThat(result, containsInAnyOrder("[\"Exasol\",\"DynamoDB\",\"Virtual Schema\"]", "[\"Fantasy\"]",
                "[\"Birds\",\"Nature\"]"));
    }

    private void createTableBooksTableWithPublisherPriceKey() throws IOException {
        final CreateTableRequest.Builder requestBuilder = CreateTableRequest.builder();
        requestBuilder.tableName(DYNAMO_BOOKS_TABLE);
        requestBuilder
                .keySchema(List.of(KeySchemaElement.builder().attributeName("publisher").keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("price").keyType(KeyType.RANGE).build()));
        requestBuilder.attributeDefinitions(List.of(
                AttributeDefinition.builder().attributeName("publisher").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("price").attributeType(ScalarAttributeType.N).build()));
        requestBuilder.provisionedThroughput(
                ProvisionedThroughput.builder().readCapacityUnits(100L).writeCapacityUnits(100L).build());
        dynamodbTestInterface.createTable(requestBuilder.build());
        dynamodbTestInterface.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
    }

    @Test
    void testDataTypes() throws IOException, SQLException {
        createDataTypesVirtualSchema();
        final ResultSet actualResultSet = exasolTestDatabaseBuilder.getStatement()
                .executeQuery("SELECT * FROM " + TEST_SCHEMA + "." + MappingTestFiles.DATA_TYPE_TEST_EXASOL_TABLE_NAME
                        + " WHERE STRINGVALUE = 'test';");
        actualResultSet.next();
        assertAll(() -> assertThat(actualResultSet.getString("STRINGVALUE"), equalTo("test")),
                () -> assertThat(actualResultSet.getString("BOOLVALUE"), equalTo("true")),
                () -> assertThat(actualResultSet.getString("DOUBLEVALUE"), equalTo("1.2")),
                () -> assertThat(actualResultSet.getString("INTEGERVALUE"), equalTo("1")),
                () -> assertThat(actualResultSet.getString("NULLVALUE"), equalTo(null))//
        );
    }

    private void createNestedTableVirtualSchema() throws SQLException, IOException {
        exasolTestDatabaseBuilder.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                BUCKETFS_PATH + MappingTestFiles.SINGLE_COLUMN_TO_TABLE_MAPPING);
        dynamodbTestInterface.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestInterface.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
    }

    private void createDoubleNestedTableVirtualSchema() throws SQLException, IOException {
        dynamodbTestInterface.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestInterface.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
        exasolTestDatabaseBuilder.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                BUCKETFS_PATH + MappingTestFiles.DOUBLE_NESTED_TO_TABLE_MAPPING);
    }

    private void createDoubleNestedTableVirtualSchemaWithCompoundPrimaryKey() throws SQLException, IOException {
        final CreateTableRequest.Builder createTableRequestBuilder = CreateTableRequest.builder();
        createTableRequestBuilder.tableName(DYNAMO_BOOKS_TABLE);
        createTableRequestBuilder
                .keySchema(
                        KeySchemaElement.builder().attributeName(TestDocuments.BOOKS_ISBN_PROPERTY)
                                .keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("name").keyType(KeyType.RANGE).build());
        createTableRequestBuilder.attributeDefinitions(
                AttributeDefinition.builder().attributeName(TestDocuments.BOOKS_ISBN_PROPERTY)
                        .attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("name").attributeType(ScalarAttributeType.S).build());
        createTableRequestBuilder.provisionedThroughput(
                ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build());
        dynamodbTestInterface.createTable(createTableRequestBuilder.build());
        dynamodbTestInterface.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
        exasolTestDatabaseBuilder.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                BUCKETFS_PATH + MappingTestFiles.DOUBLE_NESTED_TO_TABLE_MAPPING);
    }

    void createBasicMappingVirtualSchema() throws SQLException {
        exasolTestDatabaseBuilder.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                BUCKETFS_PATH + MappingTestFiles.BASIC_MAPPING);
    }

    void createDataTypesVirtualSchema() throws SQLException, IOException {
        exasolTestDatabaseBuilder.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                BUCKETFS_PATH + MappingTestFiles.DATA_TYPE_TEST_MAPPING);
        dynamodbTestInterface.createTable(MappingTestFiles.DATA_TYPE_TEST_SRC_TABLE_NAME,
                TestDocuments.DATA_TYPE_TEST_STRING_VALUE);
        dynamodbTestInterface.importData(MappingTestFiles.DATA_TYPE_TEST_SRC_TABLE_NAME, TestDocuments.dataTypeTest());
    }

    void createToJsonMappingVirtualSchema() throws SQLException {
        exasolTestDatabaseBuilder.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                BUCKETFS_PATH + MappingTestFiles.TO_JSON_MAPPING);
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
