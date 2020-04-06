package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

import java.io.File;
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
            .withNetwork(NETWORK).withExposedPorts(8888).withLogConsumer(new Slf4jLogConsumer(LOGGER));
    private static final String TEST_SCHEMA = "TEST";
    private static final String DYNAMODB_CONNECTION = "DYNAMODB_CONNECTION";
    private static final String DYNAMO_TABLE_NAME = "JB_Books";
    private static DynamodbTestUtils dynamodbTestUtils;
    private static ExasolTestUtils exasolTestUtils;

    /**
     * Creates a Virtual Schema in the Exasol test container accessing the local DynamoDB.
     */
    @BeforeAll
    static void beforeAll() throws DynamodbTestUtils.NoNetworkFoundException, SQLException, InterruptedException,
            BucketAccessException, TimeoutException {
        dynamodbTestUtils = new DynamodbTestUtils(LOCAL_DYNAMO, NETWORK);
        exasolTestUtils = new ExasolTestUtils(EXASOL_CONTAINER);
        exasolTestUtils.uploadDynamodbAdapterJar();
        exasolTestUtils.uploadMapping("basicMapping.json");
        exasolTestUtils.createAdapterScript();
        LOGGER.info("created adapter script");
        exasolTestUtils.createConnection(DYNAMODB_CONNECTION, dynamodbTestUtils.getDynamoUrl(),
                dynamodbTestUtils.getDynamoUser(), dynamodbTestUtils.getDynamoPass());
        LOGGER.info("created connection");
        exasolTestUtils.createDynamodbVirtualSchema(TEST_SCHEMA, DYNAMODB_CONNECTION,
                "/bfsdefault/default/mappings/basicMapping.json");
        LOGGER.info("created schema");
    }

    @AfterAll
    static void afterAll() {
        NETWORK.close();
    }

    @Test
    public void testSchemaDefinition() throws SQLException {
        final Map<String, String> rowNames = exasolTestUtils.describeTable(TEST_SCHEMA, "BOOKS");
        assertThat(rowNames, equalTo(
                Map.of("ISBN", "VARCHAR(20) UTF8", "NAME", "VARCHAR(100) UTF8", "AUTHOR_NAME", "VARCHAR(20) UTF8")));
    }

    /**
     * Helper function that runs a {@code SELECT *} and return a single string column. In addition the execution time is
     * measured.
     */
    private SelectStringArrayResult selectStringArray() throws SQLException {
        final long start = System.currentTimeMillis();
        final ResultSet actualResultSet = exasolTestUtils.getStatement()
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
        dynamodbTestUtils.createTable(DYNAMO_TABLE_NAME, "isbn");
        final List<String> result = selectStringArray().rows;
        assertThat(result.size(), equalTo(0));
    }

    /**
     * Tests an {@code SELECT *} from an DynamoDB table with a single line.
     */
    @Test
    void testSingleLineSelect() throws SQLException {
        dynamodbTestUtils.createTable(DYNAMO_TABLE_NAME, "isbn");
        final String Isbn = "12398439493";
        dynamodbTestUtils.putItem(DYNAMO_TABLE_NAME, Isbn, "test name");
        final SelectStringArrayResult result = selectStringArray();
        assertThat(result.rows, containsInAnyOrder(Isbn));
    }

    /**
     * Tests an {@code SELECT *} from an DynamoDB table with a single line with string result.
     */
    @Test
    void testSingleLineSelectWithStringResult() throws SQLException {
        dynamodbTestUtils.createTable(DYNAMO_TABLE_NAME, "isbn");
        final String Isbn = "abc";
        dynamodbTestUtils.putItem(DYNAMO_TABLE_NAME, Isbn, "test name");
        final SelectStringArrayResult result = selectStringArray();
        assertThat(result.rows, containsInAnyOrder(Isbn));
    }

    /**
     * Tests a {@code SELECT *} from a DynamoDB table with multiple lines.
     */
    @Test
    void testMultiLineSelect() throws IOException, SQLException {
        dynamodbTestUtils.createTable(DYNAMO_TABLE_NAME, "isbn");
        final ClassLoader classLoader = DynamodbAdapterTestLocalIT.class.getClassLoader();
        dynamodbTestUtils.importData(DYNAMO_TABLE_NAME, new File(classLoader.getResource("books.json").getFile()));
        final List<String> result = selectStringArray().rows;
        assertThat(result, containsInAnyOrder("123567", "123254545", "1235673"));
    }

    /**
     * Tests a {@code SELECT *} from a large DynamoDB table.
     */
    @Test
    void testBigScan() throws SQLException {
        dynamodbTestUtils.createTable(DYNAMO_TABLE_NAME, "isbn");
        final int numBooks = 1000;
        final List<String> actualBookNames = new ArrayList<>(numBooks);
        for (int i = 0; i < numBooks; i++) {
            final String booksName = String.valueOf(i);
            dynamodbTestUtils.putItem(DYNAMO_TABLE_NAME, booksName, "name equal for all books");
            actualBookNames.add(booksName);
        }
        final SelectStringArrayResult result = selectStringArray();
        assertThat(result.rows, containsInAnyOrder(actualBookNames.toArray()));
    }

    @AfterEach
    void after() {
        dynamodbTestUtils.deleteCreatedTables();
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
