package com.exasol.adapter.dynamodb.queryrunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.dynamodb.DynamodbTestInterface;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.DocumentObject;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.TableMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.TestDocuments;
import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.dynamodb.queryresultschema.QueryResultTableSchema;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.adapter.sql.SqlTable;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.dynamodb.DynamodbConnectionFactory;

@Tag("integration")
@Tag("quick")
@Testcontainers
class DynamodbQueryRunnerIT {
    private static final Network NETWORK = Network.newNetwork();
    @Container
    public static final GenericContainer LOCAL_DYNAMO = new GenericContainer<>("amazon/dynamodb-local")
            .withNetwork(NETWORK).withExposedPorts(8000).withNetworkAliases("dynamo")
            .withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");
    private static final String TABLE_NAME = "test";
    private static final String KEY_NAME = "isbn";
    private static DynamodbTestInterface dynamodbTestInterface;

    @BeforeAll
    static void beforeAll() throws DynamodbTestInterface.NoNetworkFoundException, SQLException, InterruptedException,
            BucketAccessException, TimeoutException, IOException {
        dynamodbTestInterface = new DynamodbTestInterface(LOCAL_DYNAMO, NETWORK);
        dynamodbTestInterface.createTable(TABLE_NAME, KEY_NAME);
        dynamodbTestInterface.importData(TABLE_NAME, TestDocuments.BOOKS);
    }

    @AfterAll
    static void afterAll() {
        NETWORK.close();
    }

    private AmazonDynamoDB getDynamodbConnection() {
        return new DynamodbConnectionFactory().getLowLevelConnection(dynamodbTestInterface.getDynamoUrl(),
                dynamodbTestInterface.getDynamoUser(), dynamodbTestInterface.getDynamoPass());
    }

    @AfterEach
    void afterEach() {
        dynamodbTestInterface.deleteCreatedTables();
    }

    private DynamodbQueryRunner getRunner() {
        return new DynamodbQueryRunner(new ExaConnectionInformation() {
            @Override
            public ConnectionType getType() {
                return null;
            }

            @Override
            public String getAddress() {
                return dynamodbTestInterface.getDynamoUrl();
            }

            @Override
            public String getUser() {
                return dynamodbTestInterface.getDynamoUser();
            }

            @Override
            public String getPassword() {
                return dynamodbTestInterface.getDynamoPass();
            }
        });
    }

    @Test
    void testSelectAll() {
        final DynamodbQueryRunner runner = getRunner();
        final ToJsonColumnMappingDefinition column1 = new ToJsonColumnMappingDefinition(
                new AbstractColumnMappingDefinition.ConstructorParameters("", null, null));
        final TableMappingDefinition table = TableMappingDefinition.rootTableBuilder("", TABLE_NAME)
                .withColumnMappingDefinition(column1).build();
        final QueryResultTableSchema queryResultTableSchema = new QueryResultTableSchema(table, List.of(column1));
        final SqlStatementSelect selectStatement = new SqlStatementSelect.Builder()
                .fromClause(new SqlTable(TABLE_NAME, null)).selectList(SqlSelectList.createSelectStarSelectList())
                .build();
        final List<DocumentNode<DynamodbNodeVisitor>> result = runner.runQuery(queryResultTableSchema, selectStatement)
                .collect(Collectors.toList());
        assertThat(result.size(), equalTo(3));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        assertThat(first.hasKey("author"), equalTo(true));
    }
}