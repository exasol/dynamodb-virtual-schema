package com.exasol.adapter.dynamodb.queryrunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.DynamodbTestInterface;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.DocumentObject;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.mapping.JsonMappingFactory;
import com.exasol.adapter.dynamodb.mapping.MappingTestFiles;
import com.exasol.adapter.dynamodb.mapping.TableMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.TestDocuments;
import com.exasol.adapter.dynamodb.queryplan.DocumentQuery;
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
    private static final String KEY_NAME = "isbn";
    private static DynamodbTestInterface dynamodbTestInterface;

    private static TableMappingDefinition tableMapping;
    private static DocumentQuery documentQuery;

    @BeforeAll
    static void beforeAll() throws DynamodbTestInterface.NoNetworkFoundException, SQLException, InterruptedException,
            BucketAccessException, TimeoutException, IOException, AdapterException {
        tableMapping = new JsonMappingFactory(MappingTestFiles.BASIC_MAPPING_FILE).getSchemaMapping().getTableMappings()
                .get(0);
        documentQuery = new DocumentQuery(tableMapping, tableMapping.getColumns());

        dynamodbTestInterface = new DynamodbTestInterface(LOCAL_DYNAMO, NETWORK);
        dynamodbTestInterface.createTable(tableMapping.getRemoteName(), KEY_NAME);
        dynamodbTestInterface.importData(tableMapping.getRemoteName(), TestDocuments.BOOKS);
    }

    @AfterAll
    static void afterAll() {
        NETWORK.close();
    }

    private AmazonDynamoDB getDynamodbConnection() {
        return new DynamodbConnectionFactory().getLowLevelConnection(dynamodbTestInterface.getDynamoUrl(),
                dynamodbTestInterface.getDynamoUser(), dynamodbTestInterface.getDynamoPass());
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
        final SqlStatementSelect selectStatement = new SqlStatementSelect.Builder()
                .fromClause(new SqlTable(tableMapping.getExasolName(), null))
                .selectList(SqlSelectList.createSelectStarSelectList()).build();
        final List<DocumentNode<DynamodbNodeVisitor>> result = runner.runQuery(documentQuery, selectStatement)
                .collect(Collectors.toList());
        assertThat(result.size(), equalTo(3));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        assertThat(first.hasKey("author"), equalTo(true));
    }
}