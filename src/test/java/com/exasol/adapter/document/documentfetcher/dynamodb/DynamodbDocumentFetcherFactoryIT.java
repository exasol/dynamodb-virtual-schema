package com.exasol.adapter.document.documentfetcher.dynamodb;

import static com.exasol.adapter.document.documentfetcher.dynamodb.BasicMappingSetup.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.document.DynamodbTestInterface;
import com.exasol.adapter.document.IntegrationTestSetup;
import com.exasol.adapter.document.documentfetcher.DocumentFetcher;
import com.exasol.adapter.document.documentfetcher.FetchedDocument;
import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.adapter.document.documentnode.DocumentObject;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbString;
import com.exasol.adapter.document.mapping.TestDocuments;
import com.exasol.adapter.document.queryplanning.RemoteTableQuery;

import software.amazon.awssdk.services.dynamodb.model.*;

@Tag("integration")
@Tag("quick")
class DynamodbDocumentFetcherFactoryIT {
    private static DynamodbTestInterface dynamodbTestInterface;
    private static BasicMappingSetup basicMappingSetup;
    @TempDir
    static Path tempDir;

    @BeforeAll
    static void beforeAll()
            throws DynamodbTestInterface.NoNetworkFoundException, IOException, AdapterException, URISyntaxException {
        final IntegrationTestSetup integrationTestSetup = new IntegrationTestSetup();
        dynamodbTestInterface = integrationTestSetup.getDynamodbTestInterface();
        basicMappingSetup = new BasicMappingSetup(tempDir);
        setupTestDatabase();
    }

    private static void setupTestDatabase() throws IOException {
        final CreateTableRequest.Builder requestBuilder = CreateTableRequest.builder();
        requestBuilder.tableName(basicMappingSetup.tableMapping.getRemoteName());
        requestBuilder
                .keySchema(KeySchemaElement.builder().attributeName(PRIMARY_KEY_NAME).keyType(KeyType.HASH).build());
        requestBuilder.attributeDefinitions(
                AttributeDefinition.builder().attributeName(PRIMARY_KEY_NAME).attributeType(ScalarAttributeType.S)
                        .build(),
                AttributeDefinition.builder().attributeName(INDEX_PARTITION_KEY).attributeType(ScalarAttributeType.S)
                        .build(),
                AttributeDefinition.builder().attributeName(INDEX_SORT_KEY).attributeType(ScalarAttributeType.N)
                        .build());
        requestBuilder.provisionedThroughput(
                ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build());
        requestBuilder.globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                .keySchema(KeySchemaElement.builder().attributeName(INDEX_PARTITION_KEY).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(INDEX_SORT_KEY).keyType(KeyType.RANGE).build())
                .provisionedThroughput(
                        ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build())
                .indexName(INDEX_NAME).projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .build());
        dynamodbTestInterface.createTable(requestBuilder.build());
        dynamodbTestInterface.importData(basicMappingSetup.tableMapping.getRemoteName(), TestDocuments.books());
    }

    @AfterAll
    static void afterAll() {
        dynamodbTestInterface.teardown();
    }

    private List<DocumentNode<DynamodbNodeVisitor>> runQueryAndExtractDocuments(final RemoteTableQuery query)
            throws URISyntaxException {
        final Stream<FetchedDocument<DynamodbNodeVisitor>> fetchedDocuments = runQuery(query);
        return fetchedDocuments.map(FetchedDocument::getRootDocumentNode).collect(Collectors.toList());
    }

    @NotNull
    private Stream<FetchedDocument<DynamodbNodeVisitor>> runQuery(final RemoteTableQuery query)
            throws URISyntaxException {
        final DynamodbDocumentFetcherFactory fetcherFactory = new DynamodbDocumentFetcherFactory(
                dynamodbTestInterface.getDynamodbLowLevelConnection());
        final List<DocumentFetcher<DynamodbNodeVisitor>> documentFetchers = fetcherFactory
                .buildDocumentFetcherForQuery(query, 2).getDocumentFetchers();
        return documentFetchers.stream()
                .flatMap((DocumentFetcher<DynamodbNodeVisitor> documentFetcher) -> documentFetcher
                        .run(dynamodbTestInterface.getExaConnectionInformationForDynamodb()));
    }

    @Test
    void testSourcePath() throws URISyntaxException {
        final RemoteTableQuery remoteTableQuery = basicMappingSetup.getSelectAllQuery();
        final Stream<FetchedDocument<DynamodbNodeVisitor>> fetchedDocuments = runQuery(remoteTableQuery);
        final String firstSourcePath = fetchedDocuments.map(FetchedDocument::getSourcePath).findFirst().orElseThrow();
        assertThat(firstSourcePath, equalTo(basicMappingSetup.tableMapping.getRemoteName()));
    }

    @Test
    void testSelectAll() throws URISyntaxException {
        final RemoteTableQuery remoteTableQuery = basicMappingSetup.getSelectAllQuery();
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQueryAndExtractDocuments(remoteTableQuery);
        assertThat(result.size(), equalTo(3));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        assertThat(first.hasKey("author"), equalTo(true));
    }

    @Test
    void testRequestSingleItem() throws URISyntaxException {
        final String isbn = "123567";
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForIsbn(isbn);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQueryAndExtractDocuments(documentQuery);
        assertThat(result.size(), equalTo(1));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        assertThat(getItemsIsbn(first), equalTo(isbn));
    }

    @Test
    void testSelectAllButASingleItem() throws URISyntaxException {
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForNotIsbn("123567");
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQueryAndExtractDocuments(documentQuery);
        final List<String> resultsIsbns = result.stream()
                .map(x -> getItemsIsbn((DocumentObject<DynamodbNodeVisitor>) x)).collect(Collectors.toList());
        assertThat(resultsIsbns, containsInAnyOrder("1235673", "123254545"));
    }

    @Test
    void testSecondaryIndexQuery() throws URISyntaxException {
        final String publisher = "jb books";
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForPublisher(publisher);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQueryAndExtractDocuments(documentQuery);
        assertThat(result.size(), equalTo(2));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        final DynamodbString resultsPublisher = (DynamodbString) first.get("publisher");
        assertThat(resultsPublisher.getValue(), equalTo(publisher));
    }

    @Test
    void testSortKeyIndexQuery() throws URISyntaxException {
        final String publisher = "jb books";
        final RemoteTableQuery query = basicMappingSetup.getQueryForMinPriceAndPublisher(11, publisher);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQueryAndExtractDocuments(query);
        assertThat(result.size(), equalTo(1));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        assertThat(getItemsIsbn(first), equalTo("123567"));
    }

    @Test
    void testSortKeyIndexQueryWithNot() throws URISyntaxException {
        final String publisher = "jb books";
        final RemoteTableQuery query = basicMappingSetup.getQueryForMaxPriceAndPublisher(11, publisher);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQueryAndExtractDocuments(query);
        assertThat(result.size(), equalTo(1));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        assertThat(getItemsIsbn(first), equalTo("123254545"));
    }

    @Test
    void testKeyAndNonKeyQuery() throws URISyntaxException {
        final String publisher = "jb books";
        final String name = "bad book 1";
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForNameAndPublisher(name, publisher);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQueryAndExtractDocuments(documentQuery);
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
    void testKeyAndNonKeyQueryWithTwoNonKeyValues() throws URISyntaxException {
        final String publisher = "jb books";
        final String name1 = "bad book 1";
        final String name2 = "bad book 2";
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForTwoNamesAndPublisher(name1, name2,
                publisher);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQueryAndExtractDocuments(documentQuery);
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
    void testRangeQuery() throws URISyntaxException {
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForMinPrice(16);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQueryAndExtractDocuments(documentQuery);
        assertThat(result.size(), equalTo(1));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        assertThat(getItemsIsbn(first), equalTo("1235673"));
    }

    @Test
    void testQueryOnIndexAndPrimaryKeyProperties() throws URISyntaxException {
        final String isbn = "123567";
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForPriceAndPublisherAndIsbn(15, "jb books",
                isbn);
        final List<DocumentNode<DynamodbNodeVisitor>> result = runQueryAndExtractDocuments(documentQuery);
        assertThat(result.size(), equalTo(1));
        final DocumentObject<DynamodbNodeVisitor> first = (DocumentObject<DynamodbNodeVisitor>) result.get(0);
        assertThat(getItemsIsbn(first), equalTo(isbn));
    }

    private String getItemsIsbn(final DocumentObject<DynamodbNodeVisitor> first) {
        return ((DynamodbString) first.get("isbn")).getValue();
    }
}