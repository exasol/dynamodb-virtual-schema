package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.adapter.dynamodb.mapping.MappingTestFiles;
import com.exasol.adapter.dynamodb.mapping.TestDocuments;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;

/**
 * Tests the {@link DynamodbAdapter} using a local docker version of DynamoDB and a local docker version of exasol.
 **/
@Tag("integration")
@Testcontainers
public class DynamodbAdapterTestLocalIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbAdapterTestLocalIT.class);

    private static final Network NETWORK = Network.newNetwork();
    @Container
    public static final GenericContainer LOCAL_DYNAMO = new GenericContainer<>("amazon/dynamodb-local")
            .withExposedPorts(8000).withNetwork(NETWORK).withNetworkAliases("dynamo")
            .withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");
    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> EXASOL_CONTAINER = new ExasolContainer<>()
            .withFileSystemBind("./target/jacoco-agent", "/jacoco-agent")
            .withFileSystemBind("./target/jacoco-report", "/jacoco-report")
            .withCommand("\"-javaagent:/jacoco-agent/org.jacoco.agent-runtime.jar=destfile=/jacoco-report/jacoco-it2.exec\"")
            .withNetwork(NETWORK).withExposedPorts(8888).withLogConsumer(new Slf4jLogConsumer(LOGGER));
    private static final String TEST_SCHEMA = "TEST";
    private static final String DYNAMODB_CONNECTION = "DYNAMODB_CONNECTION";
    private static final String DYNAMO_TABLE_NAME = "MY_BOOKS";
    private static DynamodbTestInterface dynamodbTestInterface;
    private static ExasolTestInterface exasolTestInterface;

    /**
     * Creates a Virtual Schema in the Exasol test container accessing the local DynamoDB.
     */
    @BeforeAll
    static void beforeAll() throws DynamodbTestInterface.NoNetworkFoundException, SQLException, InterruptedException,
            BucketAccessException, TimeoutException {
        dynamodbTestInterface = new DynamodbTestInterface(LOCAL_DYNAMO, NETWORK);
        exasolTestInterface = new ExasolTestInterface(EXASOL_CONTAINER);
        exasolTestInterface.uploadDynamodbAdapterJar();
        exasolTestInterface.uploadMapping(MappingTestFiles.BASIC_MAPPING_FILE_NAME);
        exasolTestInterface.createAdapterScript();
        LOGGER.info("created adapter script");
        exasolTestInterface.createConnection(DYNAMODB_CONNECTION, dynamodbTestInterface.getDynamoUrl(),
                dynamodbTestInterface.getDynamoUser(), dynamodbTestInterface.getDynamoPass());
        LOGGER.info("created connection");
        exasolTestInterface.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                "/bfsdefault/default/mappings/" + MappingTestFiles.BASIC_MAPPING_FILE_NAME);
        LOGGER.info("created schema");
    }

    @AfterAll
    static void afterAll() {
        NETWORK.close();
        EXASOL_CONTAINER.getDockerClient()
                .stopContainerCmd(EXASOL_CONTAINER.getContainerId())
                .withTimeout(10)
                .exec();
    }

    @AfterEach
    void after() {
        dynamodbTestInterface.deleteCreatedTables();
    }

    @Test
    public void testSchemaDefinition() throws SQLException {
        final Map<String, String> rowNames = exasolTestInterface.describeTable(TEST_SCHEMA, "BOOKS");
        assertThat(rowNames, equalTo(
                Map.of("ISBN", "VARCHAR(20) UTF8", "NAME", "VARCHAR(100) UTF8", "AUTHOR_NAME", "VARCHAR(20) UTF8")));
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
        dynamodbTestInterface.createTable(DYNAMO_TABLE_NAME, TestDocuments.BOOKS_ISBN_PROPERTY);
        final List<String> result = selectStringArray().rows;
        assertThat(result.size(), equalTo(0));
    }

    /**
     * Tests an {@code SELECT *} from an DynamoDB table with a single line.
     */
    @Test
    void testSingleLineSelect() throws SQLException {
        dynamodbTestInterface.createTable(DYNAMO_TABLE_NAME, TestDocuments.BOOKS_ISBN_PROPERTY);
        final String Isbn = "12398439493";
        dynamodbTestInterface.putItem(DYNAMO_TABLE_NAME, Isbn, "test name");
        final SelectStringArrayResult result = selectStringArray();
        assertThat(result.rows, containsInAnyOrder(Isbn));
    }

    /**
     * Tests an {@code SELECT *} from an DynamoDB table with a single line with string result.
     */
    @Test
    void testSingleLineSelectWithStringResult() throws SQLException {
        dynamodbTestInterface.createTable(DYNAMO_TABLE_NAME, TestDocuments.BOOKS_ISBN_PROPERTY);
        final String Isbn = "abc";
        dynamodbTestInterface.putItem(DYNAMO_TABLE_NAME, Isbn, "test name");
        final SelectStringArrayResult result = selectStringArray();
        assertThat(result.rows, containsInAnyOrder(Isbn));
    }

    /**
     * Tests a {@code SELECT *} from a DynamoDB table with multiple lines.
     */
    @Test
    void testMultiLineSelect() throws IOException, SQLException {
        dynamodbTestInterface.createTable(DYNAMO_TABLE_NAME, TestDocuments.BOOKS_ISBN_PROPERTY);
        dynamodbTestInterface.importData(DYNAMO_TABLE_NAME, TestDocuments.BOOKS);
        final List<String> result = selectStringArray().rows;
        assertThat(result, containsInAnyOrder("123567", "123254545", "1235673"));
    }

    /**
     * Tests a {@code SELECT *} from a large DynamoDB table.
     */
    @Test
    void testBigScan() throws SQLException {
        dynamodbTestInterface.createTable(DYNAMO_TABLE_NAME, TestDocuments.BOOKS_ISBN_PROPERTY);
        final int numBooks = 1000;
        final List<String> actualBookNames = new ArrayList<>(numBooks);
        for (int i = 0; i < numBooks; i++) {
            final String booksName = String.valueOf(i);
            dynamodbTestInterface.putItem(DYNAMO_TABLE_NAME, booksName, "name equal for all books");
            actualBookNames.add(booksName);
        }
        final SelectStringArrayResult result = selectStringArray();
        assertThat(result.rows, containsInAnyOrder(actualBookNames.toArray()));
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
