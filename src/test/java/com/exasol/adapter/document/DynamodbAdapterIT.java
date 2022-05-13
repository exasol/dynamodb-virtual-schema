package com.exasol.adapter.document;

import static com.exasol.adapter.document.GenericUdfCallHandler.*;
import static com.exasol.adapter.document.JsonHelper.toJson;
import static com.exasol.matcher.ResultSetStructureMatcher.table;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.adapter.document.mapping.MappingTestFiles;
import com.exasol.bucketfs.Bucket;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.dbbuilder.dialects.exasol.*;
import com.exasol.dbbuilder.dialects.exasol.udf.UdfScript;
import com.exasol.dynamodb.DynamodbContainer;
import com.exasol.udfdebugging.PushDownTesting;
import com.exasol.udfdebugging.UdfTestSetup;
import com.github.dockerjava.api.model.ContainerNetwork;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Tests the DynamoDB virtual schema adapter using a local docker version of DynamoDB and a local docker version of
 * Exasol.
 **/
@Tag("integration")
@Testcontainers
class DynamodbAdapterIT {
    public static final String BUCKETS_BFSDEFAULT_DEFAULT = "/buckets/bfsdefault/default/";
    private static final String JAR_NAME_AND_VERSION = "document-virtual-schema-dist-9.0.0-dynamodb-3.0.0.jar";
    private static final Path PATH_TO_VIRTUAL_SCHEMAS_JAR = Path.of("target", JAR_NAME_AND_VERSION);
    private static final String LOCAL_DYNAMO_USER = "fakeMyKeyId";
    private static final String LOCAL_DYNAMO_PASS = "fakeSecretAccessKey";
    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> EXASOL = new ExasolContainer<>().withReuse(true);
    @Container
    private static final DynamodbContainer DYNAMODB = new DynamodbContainer();

    private static final String TEST_SCHEMA = "TEST";
    private static final String LIST_TABLES_QUERY = "SELECT TABLE_NAME FROM EXA_ALL_VIRTUAL_TABLES WHERE TABLE_SCHEMA = '"
            + TEST_SCHEMA + "'";
    private static final String DYNAMO_BOOKS_TABLE = "MY_BOOKS";
    private static final String DIFFERENT_RESULT_TYPE_MAPPING = "differentResultTypesMapping.json";
    public static final List<String> REQUIRED_MAPPINGS = List.of(MappingTestFiles.BASIC_MAPPING,
            MappingTestFiles.SINGLE_COLUMN_TO_TABLE_MAPPING, MappingTestFiles.DATA_TYPE_TEST_MAPPING,
            MappingTestFiles.DOUBLE_NESTED_TO_TABLE_MAPPING, MappingTestFiles.TO_JSON_MAPPING,
            DIFFERENT_RESULT_TYPE_MAPPING);
    private static DynamodbTestDbBuilder dynamodbTestDbBuilder;
    private static Statement statement;
    private static ExasolObjectFactory testDbBuilder;
    private static AdapterScript adapterScript;
    private static ConnectionDefinition connectionDefinition;
    private final List<VirtualSchema> createdVirtualSchemas = new LinkedList<>();

    private static void uploadAdapter() throws BucketAccessException, TimeoutException, FileNotFoundException {
        EXASOL.getDefaultBucket().uploadFile(PATH_TO_VIRTUAL_SCHEMAS_JAR, JAR_NAME_AND_VERSION);
    }

    /**
     * Create a Virtual Schema in the Exasol test container accessing the local DynamoDB.
     */
    @BeforeAll
    static void beforeAll() throws Exception {
        dynamodbTestDbBuilder = new TestcontainerDynamodbTestDbBuilder(DYNAMODB);
        uploadAdapter();
        final Connection connection = EXASOL.createConnection();
        final UdfTestSetup udfTestSetup = new UdfTestSetup(getTestHostIpFromInsideExasol(), EXASOL.getDefaultBucket(),
                connection);
        testDbBuilder = new ExasolObjectFactory(connection,
                ExasolObjectConfiguration.builder().withJvmOptions(udfTestSetup.getJvmOptions()).build());
        final ExasolSchema adapterSchema = testDbBuilder.createSchema("ADAPTER");
        adapterScript = adapterSchema.createAdapterScriptBuilder("DYNAMODB_ADAPTER")
                .bucketFsContent("com.exasol.adapter.RequestDispatcher",
                        BUCKETS_BFSDEFAULT_DEFAULT + JAR_NAME_AND_VERSION)
                .language(AdapterScript.Language.JAVA).build();
        adapterSchema.createUdfBuilder("IMPORT_FROM_DYNAMO_DB").language(UdfScript.Language.JAVA)
                .inputType(UdfScript.InputType.SET).parameter(PARAMETER_DOCUMENT_FETCHER, "VARCHAR(2000000)")
                .parameter(PARAMETER_SCHEMA_MAPPING_REQUEST, "VARCHAR(2000000)")
                .parameter(PARAMETER_CONNECTION_NAME, "VARCHAR(500)").emits()
                .bucketFsContent(UdfEntryPoint.class.getName(), "/buckets/bfsdefault/default/" + JAR_NAME_AND_VERSION)
                .build();
        connectionDefinition = testDbBuilder.createConnectionDefinition("DYNAMODB_CONNECTION", "", "",
                getExaConnectionInformationForDynamodb());

        for (final String mapping : REQUIRED_MAPPINGS) {
            EXASOL.getDefaultBucket().uploadInputStream(
                    () -> DynamodbAdapterIT.class.getClassLoader().getResourceAsStream(mapping), mapping);
        }
        statement = connection.createStatement();
    }

    private static String getExaConnectionInformationForDynamodb() {
        final JsonObject jsonConfig = Json.createObjectBuilder().add("awsAccessKeyId", LOCAL_DYNAMO_USER)
                .add("awsSecretAccessKey", LOCAL_DYNAMO_PASS)
                .add("awsEndpointOverride", getTestHostIpFromInsideExasol() + ":" + DYNAMODB.getPort())
                .add("awsRegion", "eu-central-1").add("useSsl", false).build();
        return toJson(jsonConfig);
    }

    private static String getTestHostIpFromInsideExasol() {
        final Map<String, ContainerNetwork> networks = EXASOL.getContainerInfo().getNetworkSettings().getNetworks();
        if (networks.size() == 0) {
            return null;
        }
        return networks.values().iterator().next().getGateway();
    }

    @AfterEach
    void afterEach() {
        dynamodbTestDbBuilder.deleteCreatedTables();
        for (final VirtualSchema virtualSchema : this.createdVirtualSchemas) {
            virtualSchema.drop();
        }
        this.createdVirtualSchemas.clear();
    }

    /**
     * Tests a {@code SELECT *} from a DynamoDB table with multiple lines.
     */
    @Test
    void testDataLoading() throws IOException, SQLException {
        createBasicMappingVirtualSchema();
        dynamodbTestDbBuilder.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestDbBuilder.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
        final ResultSet result = statement
                .executeQuery("SELECT ISBN, NAME, AUTHOR_NAME, SOURCE_REFERENCE, PUBLISHER, PRICE FROM " + TEST_SCHEMA
                        + ".\"BOOKS\" ORDER BY PRICE ASC;");
        assertThat(result,
                table().row("123254545", "bad book 2", "Jakob Braun", "MY_BOOKS", "jb books", 10)
                        .row("123567", "bad book 1", "Jakob Braun", "MY_BOOKS", "jb books", 15)
                        .row("1235673", "boring book", "Jakob Braun", "MY_BOOKS", "no name", 21.12).matchesFuzzily());
    }

    @Test
    void testResultHasCorrectDataTypes() throws SQLException {
        createDynamodbVirtualSchema(DIFFERENT_RESULT_TYPE_MAPPING);
        final ResultSet resultSet = statement.executeQuery(
                "SELECT COLUMN_NAME, COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_TABLE = 'DIFFERENT_RESULT_TYPE_TABLE' ORDER BY COLUMN_NAME ASC");
        assertThat(resultSet, table().row("DECIMAL_COLUMN", "DECIMAL(11,3)")//
                .row("JSON_COLUMN", "VARCHAR(1000) UTF8")//
                .row("VARCHAR_COLUMN", "VARCHAR(20) UTF8").matches());
    }

    @Test
    void testAnyColumnProjection() throws SQLException, IOException {
        createBasicMappingVirtualSchema();
        dynamodbTestDbBuilder.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestDbBuilder.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
        final ResultSet result = statement
                .executeQuery("SELECT COUNT(*) as NUMBER_OF_BOOKS FROM " + TEST_SCHEMA + ".BOOKS;");
        assertThat(result, table().row(3L).matchesFuzzily());
    }

    @Test
    void testSelectNestedTableResult() throws SQLException, IOException {
        createNestedTableVirtualSchema();
        final ResultSet result = statement
                .executeQuery("SELECT BOOKS_ISBN, NAME FROM " + TEST_SCHEMA + ".\"BOOKS_TOPICS\" ORDER BY NAME ASC;");
        assertThat(result, table().row("1235673", "Birds").row("123567", "DynamoDB").row("123567", "Exasol")
                .row("123254545", "Fantasy").row("1235673", "Nature").row("123567", "Virtual Schema").matches());
    }

    // TODO refactor
    @Test
    void testJoinOnNestedTable() throws IOException, SQLException {
        createNestedTableVirtualSchema();
        final ResultSet result = statement.executeQuery("SELECT BOOKS_TOPICS.NAME FROM " + TEST_SCHEMA + ".BOOKS JOIN "
                + TEST_SCHEMA + ".\"BOOKS_TOPICS\" ON ISBN = BOOKS_ISBN WHERE BOOKS.NAME = 'bad book 1';");
        assertThat(result, table().row("Exasol").row("DynamoDB").row("Virtual Schema").matches());
    }

    @Test
    void testSelectionOnNestedTable() throws IOException {
        createNestedTableVirtualSchema();
        final String query = "SELECT BOOKS_TOPICS.NAME as TOPIC FROM " + TEST_SCHEMA
                + ".\"BOOKS_TOPICS\" WHERE NAME =  'Exasol'";
        assertAll(//
                () -> assertThat(PushDownTesting.getSelectionThatIsSentToTheAdapter(statement, query),
                        equalTo("BOOKS_TOPICS.NAME='Exasol'")),
                () -> assertThat(PushDownTesting.getPushDownSql(statement, query), endsWith("\"NAME\" = 'Exasol'")),
                () -> assertThat(statement.executeQuery(query), table().row("Exasol").matches())//
        );
    }

    // TODO refactor
    @Test
    void testJoinOnDoubleNestedTable() throws IOException, SQLException {
        createDoubleNestedTableVirtualSchema();
        final ResultSet result = statement.executeQuery("SELECT BOOKS_CHAPTERS_FIGURES.NAME FROM " + TEST_SCHEMA
                + ".BOOKS JOIN " + TEST_SCHEMA + ".\"BOOKS_CHAPTERS\" ON ISBN = BOOKS_CHAPTERS.BOOKS_ISBN " + "JOIN "
                + TEST_SCHEMA
                + ".BOOKS_CHAPTERS_FIGURES ON BOOKS_CHAPTERS.INDEX = BOOKS_CHAPTERS_INDEX AND ISBN = BOOKS_CHAPTERS_FIGURES.BOOKS_ISBN "
                + "WHERE BOOKS.NAME = 'bad book 1';");
        assertThat(result, table().row("Image of the Author").row("figure 2").row("figure 3").matches());
    }

    @Test
    void testSelectOnIndexColumn() throws IOException {
        createDoubleNestedTableVirtualSchema();
        final String query = "SELECT NAME FROM " + TEST_SCHEMA + ".BOOKS_CHAPTERS "
                + "WHERE \"INDEX\"=0 ORDER BY NAME ASC";
        assertAll(//
                () -> assertThat(PushDownTesting.getSelectionThatIsSentToTheAdapter(statement, query),
                        equalTo("BOOKS_CHAPTERS.INDEX=0")),
                () -> assertThat(PushDownTesting.getPushDownSql(statement, query), endsWith("WHERE \"INDEX\" = 0")),
                () -> assertThat(statement.executeQuery(query), table().row("Main Chapter").row("chapter 1").matches())//
        );
    }

    @Test
    void testProjectionOnPropertyInList() throws SQLException, IOException {
        createDoubleNestedTableVirtualSchema();
        final ResultSet result = statement
                .executeQuery("SELECT NAME FROM " + TEST_SCHEMA + ".BOOKS_CHAPTERS ORDER BY NAME ASC");
        assertThat(result, table().row("Main Chapter").row("chapter 1").row("chapter 2").matches());
    }

    @Test
    void testSelectOnIndexAndOtherColumn() throws IOException {
        createDoubleNestedTableVirtualSchema();
        final String query = "SELECT NAME FROM " + TEST_SCHEMA + ".BOOKS_CHAPTERS "
                + "WHERE \"INDEX\" = 0 AND NAME = 'Main Chapter'";
        assertAll(//
                () -> assertThat(PushDownTesting.getSelectionThatIsSentToTheAdapter(statement, query),
                        equalTo("(BOOKS_CHAPTERS.INDEX=0) AND (BOOKS_CHAPTERS.NAME='Main Chapter')")),
                () -> assertThat(PushDownTesting.getPushDownSql(statement, query),
                        anyOf(endsWith("(\"INDEX\" = 0) AND (\"NAME\" = 'Main Chapter')"),
                                endsWith("(\"NAME\" = 'Main Chapter') AND (\"INDEX\" = 0)"))),
                () -> assertThat(statement.executeQuery(query), table().row("Main Chapter").matches())//
        );
    }

    @Test
    void testNestedTableWithCompoundForeignKey() throws IOException, SQLException {
        createDoubleNestedTableVirtualSchemaWithCompoundPrimaryKey();
        final String query = "SELECT NAME, BOOKS_ISBN, BOOKS_NAME, \"INDEX\" FROM " + TEST_SCHEMA
                + ".BOOKS_CHAPTERS ORDER BY NAME ASC;";
        assertThat(statement.executeQuery(query), table("VARCHAR", "VARCHAR", "VARCHAR", "INTEGER")//
                .row("Main Chapter", "123254545", "bad book 2", 0)//
                .row("chapter 1", "123567", "bad book 1", 0)//
                .row("chapter 2", "123567", "bad book 1", 1)//
                .matches());
    }

    /**
     * Test that the WHERE claus filters as expected and that the adapter filters it-self and does not send delegates
     * the selection to the database.
     *
     * @throws IOException if upload fails
     */
    @Test
    void testSelection() throws IOException {
        final String selectedIsbn = "123567";
        createBasicMappingVirtualSchema();
        dynamodbTestDbBuilder.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestDbBuilder.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
        final String query = "SELECT ISBN FROM " + TEST_SCHEMA + ".\"BOOKS\" WHERE ISBN = '" + selectedIsbn + "'";
        assertAll(//
                () -> assertThat(PushDownTesting.getSelectionThatIsSentToTheAdapter(statement, query),
                        endsWith("BOOKS.ISBN='123567'")),
                () -> assertThat(PushDownTesting.getPushDownSql(statement, query), endsWith("WHERE TRUE")),
                () -> assertThat(statement.executeQuery(query), table().row(selectedIsbn).matchesFuzzily())//
        );
    }

    @Test
    void testNotSelection() throws IOException {
        createBasicMappingVirtualSchema();
        dynamodbTestDbBuilder.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestDbBuilder.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
        final String query = "SELECT ISBN FROM " + TEST_SCHEMA
                + ".\"BOOKS\" WHERE NOT(ISBN = '123567') ORDER BY ISBN ASC";
        assertAll(//
                () -> assertThat(PushDownTesting.getSelectionThatIsSentToTheAdapter(statement, query),
                        endsWith("NOT (BOOKS.ISBN='123567')")),
                () -> assertThat(PushDownTesting.getPushDownSql(statement, query), endsWith("WHERE TRUE")),
                () -> assertThat(statement.executeQuery(query),
                        table().row("123254545").row("1235673").matchesFuzzily())//
        );
    }

    @Test
    void testGreaterSelectionWithSortKey() throws SQLException, IOException {
        createBasicMappingVirtualSchema();
        createTableBooksTableWithPublisherPriceKey();
        final ResultSet actualResultSet = statement.executeQuery(
                "SELECT ISBN FROM " + TEST_SCHEMA + ".\"BOOKS\" WHERE PUBLISHER = 'jb books' AND PRICE > 10;");
        assertThat(actualResultSet, table().row("123567").matchesFuzzily());
    }

    @Test
    void testLessSelectionWithSortKey() throws SQLException, IOException {
        createBasicMappingVirtualSchema();
        createTableBooksTableWithPublisherPriceKey();
        final String query = "SELECT ISBN FROM " + TEST_SCHEMA
                + ".\"BOOKS\" WHERE PUBLISHER = 'jb books' AND PRICE < 11";
        final ResultSet actualResultSet = statement.executeQuery(query);
        assertAll(//
                () -> assertThat(PushDownTesting.getSelectionThatIsSentToTheAdapter(statement, query),
                        equalTo("(BOOKS.PUBLISHER='jb books') AND (BOOKS.PRICE<11)")),
                () -> assertThat(PushDownTesting.getPushDownSql(statement, query), endsWith("WHERE TRUE")),
                () -> assertThat(actualResultSet, table().row("123254545").matchesFuzzily())//
        );
    }

    @Test
    void testToJsonMapping() throws IOException, SQLException {
        createToJsonMappingVirtualSchema();
        dynamodbTestDbBuilder.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestDbBuilder.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
        final ResultSet result = statement
                .executeQuery("SELECT TOPICS FROM " + TEST_SCHEMA + ".BOOKS ORDER BY TOPICS ASC");
        assertThat(result, table("VARCHAR").row("[\"Birds\",\"Nature\"]")
                .row("[\"Exasol\",\"DynamoDB\",\"Virtual Schema\"]").row("[\"Fantasy\"]").matches());
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
        dynamodbTestDbBuilder.createTable(requestBuilder.build());
        dynamodbTestDbBuilder.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
    }

    @Test
    void testConvertDifferentDynamodbTypesToVarchar() throws IOException, SQLException {
        createDataTypesVirtualSchema();
        final ResultSet actualResultSet = statement.executeQuery("SELECT * FROM " + TEST_SCHEMA + "."
                + MappingTestFiles.DATA_TYPE_TEST_EXASOL_TABLE_NAME + " WHERE STRINGVALUE = 'test';");
        actualResultSet.next();
        assertAll(() -> assertThat(actualResultSet.getString("STRINGVALUE"), equalTo("test")),
                () -> assertThat(actualResultSet.getString("BOOLVALUE"), equalTo("true")),
                () -> assertThat(actualResultSet.getString("DOUBLEVALUE"), equalTo("1.2")),
                () -> assertThat(actualResultSet.getString("INTEGERVALUE"), equalTo("1")),
                () -> assertThat(actualResultSet.getString("NULLVALUE"), equalTo(null))//
        );
    }

    @Test
    void testSchemaDefinitionDoesNotChangeUntilRefresh()
            throws InterruptedException, BucketAccessException, TimeoutException, SQLException {
        final String mappingName = "mappingForReplaceTest.json";
        uploadEmptyMappingWithTable(mappingName, "T1");
        createDynamodbVirtualSchema(mappingName);
        assertThat(statement.executeQuery(LIST_TABLES_QUERY), table().row("T1").matches());
        uploadEmptyMappingWithTable(mappingName, "T2");
        assertThat(statement.executeQuery(LIST_TABLES_QUERY), table().row("T1").matches());
    }

    @Test
    void testSchemaDefinitionChangesOnRefresh()
            throws InterruptedException, BucketAccessException, TimeoutException, SQLException {
        final String mappingName = "mappingForReplaceTest.json";
        uploadEmptyMappingWithTable(mappingName, "T1");
        createDynamodbVirtualSchema(mappingName);
        assertThat(statement.executeQuery(LIST_TABLES_QUERY), table().row("T1").matches());
        uploadEmptyMappingWithTable(mappingName, "T2");
        statement.executeUpdate("ALTER VIRTUAL SCHEMA " + TEST_SCHEMA + " REFRESH");
        assertThat(statement.executeQuery(LIST_TABLES_QUERY), table().row("T2").matches());
    }

    private void uploadEmptyMappingWithTable(final String mappingName, final String tableName)
            throws InterruptedException, BucketAccessException, TimeoutException {
        final Bucket bucket = EXASOL.getDefaultBucket();
        bucket.uploadStringContent(
                "{\"$schema\": \"../../main/resources/schemas/edml-1.2.0.json\", \"source\": \"TEST\", \"destinationTable\": \""
                        + tableName + "\", \"mapping\":{ \"toJsonMapping\":{\"destinationName\":\"test\"}}}",
                mappingName);
    }

    private void createNestedTableVirtualSchema() throws IOException {
        createDynamodbVirtualSchema(MappingTestFiles.SINGLE_COLUMN_TO_TABLE_MAPPING);
        dynamodbTestDbBuilder.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestDbBuilder.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
    }

    private void createDynamodbVirtualSchema(final String mappingName) {
        this.createdVirtualSchemas.add(testDbBuilder.createVirtualSchemaBuilder(TEST_SCHEMA)
                .adapterScript(adapterScript).connectionDefinition(connectionDefinition)
                .properties(Map.of("MAPPING", "/bfsdefault/default/" + mappingName)).build());
    }

    private void createDoubleNestedTableVirtualSchema() throws IOException {
        dynamodbTestDbBuilder.createTable(DYNAMO_BOOKS_TABLE, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestDbBuilder.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
        createDynamodbVirtualSchema(MappingTestFiles.DOUBLE_NESTED_TO_TABLE_MAPPING);
    }

    private void createDoubleNestedTableVirtualSchemaWithCompoundPrimaryKey() throws IOException {
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
        dynamodbTestDbBuilder.createTable(createTableRequestBuilder.build());
        dynamodbTestDbBuilder.importData(DYNAMO_BOOKS_TABLE, TestDocuments.books());
        createDynamodbVirtualSchema(MappingTestFiles.DOUBLE_NESTED_TO_TABLE_MAPPING);
    }

    void createBasicMappingVirtualSchema() {
        createDynamodbVirtualSchema(MappingTestFiles.BASIC_MAPPING);
    }

    void createDataTypesVirtualSchema() throws IOException {
        createDynamodbVirtualSchema(MappingTestFiles.DATA_TYPE_TEST_MAPPING);
        dynamodbTestDbBuilder.createTable(MappingTestFiles.DATA_TYPE_TEST_SRC_TABLE_NAME,
                TestDocuments.DATA_TYPE_TEST_STRING_VALUE);
        dynamodbTestDbBuilder.importData(MappingTestFiles.DATA_TYPE_TEST_SRC_TABLE_NAME, TestDocuments.dataTypeTest());
    }

    void createToJsonMappingVirtualSchema() {
        createDynamodbVirtualSchema(MappingTestFiles.TO_JSON_MAPPING);
    }
}
