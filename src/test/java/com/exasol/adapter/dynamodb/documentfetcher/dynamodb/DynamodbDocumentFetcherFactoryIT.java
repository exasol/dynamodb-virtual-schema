package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import static com.exasol.adapter.dynamodb.documentfetcher.dynamodb.BasicMappingSetup.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.amazonaws.services.dynamodbv2.model.*;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.DynamodbTestInterface;
import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcher;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.DocumentObject;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbString;
import com.exasol.adapter.dynamodb.mapping.TestDocuments;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;

@Tag("integration")
@Tag("quick")
@Testcontainers
class DynamodbDocumentFetcherFactoryIT {
    private static final Network NETWORK = Network.newNetwork();
    @Container
    public static final GenericContainer LOCAL_DYNAMO = new GenericContainer<>("amazon/dynamodb-local")
            .withNetwork(NETWORK).withExposedPorts(8000).withNetworkAliases("dynamo")
            .withCommand("-jar DynamoDBLocal.jar -sharedDb -dbPath .");
    private static DynamodbTestInterface dynamodbTestInterface;
    private static BasicMappingSetup basicMappingSetup;

    @BeforeAll
    static void beforeAll() throws DynamodbTestInterface.NoNetworkFoundException, IOException, AdapterException {
        dynamodbTestInterface = new DynamodbTestInterface(LOCAL_DYNAMO, NETWORK);
        basicMappingSetup = new BasicMappingSetup();
        setupTestDatabase();
    }

    private static void setupTestDatabase() throws IOException, AdapterException {
        final CreateTableRequest request = new CreateTableRequest()
                .withTableName(basicMappingSetup.tableMapping.getRemoteName())
                .withKeySchema(new KeySchemaElement(PRIMARY_KEY_NAME, KeyType.HASH))
                .withAttributeDefinitions(new AttributeDefinition(PRIMARY_KEY_NAME, ScalarAttributeType.S),
                        new AttributeDefinition(INDEX_PARTITION_KEY, ScalarAttributeType.S),
                        new AttributeDefinition(INDEX_SORT_KEY, ScalarAttributeType.N))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))//
                .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
                        .withKeySchema(new KeySchemaElement(INDEX_PARTITION_KEY, KeyType.HASH),
                                new KeySchemaElement(INDEX_SORT_KEY, KeyType.RANGE))
                        .withIndexName(INDEX_NAME)
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)));
        dynamodbTestInterface.createTable(request);
        dynamodbTestInterface.importData(basicMappingSetup.tableMapping.getRemoteName(), TestDocuments.BOOKS);
    }

    @AfterAll
    static void afterAll() {
        NETWORK.close();
    }

    private List<DocumentNode<DynamodbNodeVisitor>> runQuery(final RemoteTableQuery<DynamodbNodeVisitor> query) {
        final DynamodbDocumentFetcherFactory fetcherFactory = new DynamodbDocumentFetcherFactory(
                dynamodbTestInterface.getDynamodbLowLevelConnection());
        final DocumentFetcher<DynamodbNodeVisitor> documentFetcher = fetcherFactory.buildDocumentFetcherForQuery(query);
        return documentFetcher.run(dynamodbTestInterface.getExaConnectionInformationForDynamodb())
                .collect(Collectors.toList());
    }

    @Test
    void testSelectAll() {
        final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery = basicMappingSetup.getSelectAllQuery();
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQuery(remoteTableQuery);
        assertThat(result.size(), equalTo(3));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        assertThat(first.hasKey("author"), equalTo(true));
    }

    @Test
    void testRequestSingleItem() {
        final String isbn = "123567";
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup.getQueryForIsbn(isbn);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQuery(documentQuery);
        assertThat(result.size(), equalTo(1));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        assertThat(getItemsIsbn(first), equalTo(isbn));
    }

    @Test
    void testSelectAllButASingleItem() {
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup.getQueryForNotIsbn("123567");
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQuery(documentQuery);
        final List<String> resultsIsbns = result.stream()
                .map(x -> getItemsIsbn((DocumentObject<DynamodbNodeVisitor>) x)).collect(Collectors.toList());
        assertThat(resultsIsbns, containsInAnyOrder("1235673", "123254545"));
    }

    @Test
    void testSecondaryIndexQuery() {
        final String publisher = "jb books";
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup.getQueryForPublisher(publisher);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQuery(documentQuery);
        assertThat(result.size(), equalTo(2));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        final DynamodbString resultsPublisher = (DynamodbString) first.get("publisher");
        assertThat(resultsPublisher.getValue(), equalTo(publisher));
    }

    @Test
    void testSortKeyIndexQuery() {
        final String publisher = "jb books";
        final RemoteTableQuery<DynamodbNodeVisitor> query = basicMappingSetup.getQueryForMinPriceAndPublisher(11,
                publisher);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQuery(query);
        assertThat(result.size(), equalTo(1));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        assertThat(getItemsIsbn(first), equalTo("123567"));
    }

    @Test
    void testSortKeyIndexQueryWithNot() {
        final String publisher = "jb books";
        final RemoteTableQuery<DynamodbNodeVisitor> query = basicMappingSetup.getQueryForMaxPriceAndPublisher(11,
                publisher);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQuery(query);
        assertThat(result.size(), equalTo(1));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        assertThat(getItemsIsbn(first), equalTo("123254545"));
    }

    @Test
    void testKeyAndNonKeyQuery() {
        final String publisher = "jb books";
        final String name = "bad book 1";
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup.getQueryForNameAndPublisher(name,
                publisher);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQuery(documentQuery);
        assertThat(result.size(), equalTo(1));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        final DynamodbString resultsPublisher = (DynamodbString) first.get("publisher");
        final DynamodbString resultsName = (DynamodbString) first.get("name");
        assertAll(//
                () -> assertThat(resultsPublisher.getValue(), equalTo(publisher)),
                () -> assertThat(resultsName.getValue(), equalTo(name))//
        );
    }

    @Test
    void testKeyAndNonKeyQueryWithTwoNonKeyValues() {
        final String publisher = "jb books";
        final String name1 = "bad book 1";
        final String name2 = "bad book 2";
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup
                .getQueryForTwoNamesAndPublisher(name1, name2, publisher);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQuery(documentQuery);
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        final DynamodbString resultsPublisher = (DynamodbString) first.get("publisher");
        final List<String> resultsNames = result.stream()
                .map(each -> ((DynamodbString) ((DocumentObject<DynamodbNodeVisitor>) each).get("name")).getValue())
                .collect(Collectors.toList());
        assertAll(//
                () -> assertThat(resultsPublisher.getValue(), equalTo(publisher)),
                () -> assertThat(resultsNames, containsInAnyOrder(name1, name2))//
        );
    }

    @Test
    void testRangeQuery() {
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup.getQueryForMinPrice(16);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQuery(documentQuery);
        assertThat(result.size(), equalTo(1));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        assertThat(getItemsIsbn(first), equalTo("1235673"));
    }

    @Test
    void testQueryOnIndexAndPrimaryKeyProperties() {
        final String isbn = "123567";
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup
                .getQueryForPriceAndPublisherAndIsbn("15", "jb books", isbn);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQuery(documentQuery);
        assertThat(result.size(), equalTo(1));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        assertThat(getItemsIsbn(first), equalTo(isbn));
    }

    private String getItemsIsbn(final DocumentObject<DynamodbNodeVisitor> first) {
        return ((DynamodbString) first.get("isbn")).getValue();
    }
}