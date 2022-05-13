package com.exasol.adapter.document.documentfetcher.dynamodb;

import static com.exasol.adapter.document.documentfetcher.dynamodb.BasicMappingSetup.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.adapter.document.*;
import com.exasol.adapter.document.connection.ConnectionPropertiesReader;
import com.exasol.adapter.document.documentfetcher.DocumentFetcher;
import com.exasol.adapter.document.documentfetcher.FetchedDocument;
import com.exasol.adapter.document.documentnode.*;
import com.exasol.adapter.document.iterators.CloseableIterator;
import com.exasol.adapter.document.queryplanning.RemoteTableQuery;
import com.exasol.dynamodb.DynamodbContainer;

import software.amazon.awssdk.services.dynamodb.model.*;

@Tag("integration")
@Tag("quick")
@Testcontainers
class DynamodbDocumentFetcherFactoryIT {
    @Container
    private static final DynamodbContainer DYNAMODB = new DynamodbContainer();
    private static BasicMappingSetup basicMappingSetup;
    private static DynamodbTestDbBuilder dynamodbTestDbBuilder;

    @BeforeAll
    static void beforeAll() throws IOException, URISyntaxException {
        dynamodbTestDbBuilder = new TestcontainerDynamodbTestDbBuilder(DYNAMODB);
        basicMappingSetup = new BasicMappingSetup();
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
        dynamodbTestDbBuilder.createTable(requestBuilder.build());
        dynamodbTestDbBuilder.importData(basicMappingSetup.tableMapping.getRemoteName(), TestDocuments.books());
    }

    private List<DocumentNode> runQueryAndExtractDocuments(final RemoteTableQuery query) throws URISyntaxException {
        final List<FetchedDocument> fetchedDocuments = runQuery(query);
        return fetchedDocuments.stream().map(FetchedDocument::getRootDocumentNode).collect(Collectors.toList());
    }

    private List<FetchedDocument> runQuery(final RemoteTableQuery query) throws URISyntaxException {
        final DynamodbDocumentFetcherFactory fetcherFactory = new DynamodbDocumentFetcherFactory(
                dynamodbTestDbBuilder.getDynamodbLowLevelConnection());
        final List<DocumentFetcher> documentFetchers = fetcherFactory.buildDocumentFetcherForQuery(query, 2)
                .getDocumentFetchers();
        final List<FetchedDocument> result = new ArrayList<>();
        for (final DocumentFetcher documentFetcher : documentFetchers) {
            try (final CloseableIterator<FetchedDocument> iterator = documentFetcher.run(new ConnectionPropertiesReader(
                    dynamodbTestDbBuilder.getExaConnectionInformationForDynamodb(), "inline"))) {
                iterator.forEachRemaining(result::add);
            }
        }
        return result;
    }

    @Test
    void testSourcePath() throws URISyntaxException {
        final RemoteTableQuery remoteTableQuery = basicMappingSetup.getSelectAllQuery();
        final List<FetchedDocument> fetchedDocuments = runQuery(remoteTableQuery);
        final String firstSourcePath = fetchedDocuments.stream().map(FetchedDocument::getSourcePath).findFirst()
                .orElseThrow();
        assertThat(firstSourcePath, equalTo(basicMappingSetup.tableMapping.getRemoteName()));
    }

    @Test
    void testSelectAll() throws URISyntaxException {
        final RemoteTableQuery remoteTableQuery = basicMappingSetup.getSelectAllQuery();
        final List<DocumentNode> result = runQueryAndExtractDocuments(remoteTableQuery);
        assertThat(result.size(), equalTo(3));
        final DocumentObject first = (DocumentObject) result.get(0);
        assertThat(first.hasKey("author"), equalTo(true));
    }

    @Test
    void testRequestSingleItem() throws URISyntaxException {
        final String isbn = "123567";
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForIsbn(isbn);
        final List<DocumentNode> result = runQueryAndExtractDocuments(documentQuery);
        assertThat(result.size(), equalTo(1));
        final DocumentObject first = (DocumentObject) result.get(0);
        assertThat(getItemsIsbn(first), equalTo(isbn));
    }

    @Test
    void testSelectAllButASingleItem() throws URISyntaxException {
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForNotIsbn("123567");
        final List<DocumentNode> result = runQueryAndExtractDocuments(documentQuery);
        final List<String> resultsIsbns = result.stream().map(x -> getItemsIsbn((DocumentObject) x))
                .collect(Collectors.toList());
        assertThat(resultsIsbns, containsInAnyOrder("1235673", "123254545"));
    }

    @Test
    void testSecondaryIndexQuery() throws URISyntaxException {
        final String publisher = "jb books";
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForPublisher(publisher);
        final List<DocumentNode> result = runQueryAndExtractDocuments(documentQuery);
        assertThat(result.size(), equalTo(2));
        final DocumentObject first = (DocumentObject) result.get(0);
        final DocumentStringValue resultsPublisher = (DocumentStringValue) first.get("publisher");
        assertThat(resultsPublisher.getValue(), equalTo(publisher));
    }

    @Test
    void testSortKeyIndexQuery() throws URISyntaxException {
        final String publisher = "jb books";
        final RemoteTableQuery query = basicMappingSetup.getQueryForMinPriceAndPublisher(11, publisher);
        final List<DocumentNode> result = runQueryAndExtractDocuments(query);
        assertThat(result.size(), equalTo(1));
        final DocumentObject first = (DocumentObject) result.get(0);
        assertThat(getItemsIsbn(first), equalTo("123567"));
    }

    @Test
    void testSortKeyIndexQueryWithNot() throws URISyntaxException {
        final String publisher = "jb books";
        final RemoteTableQuery query = basicMappingSetup.getQueryForMaxPriceAndPublisher(11, publisher);
        final List<DocumentNode> result = runQueryAndExtractDocuments(query);
        assertThat(result.size(), equalTo(1));
        final DocumentObject first = (DocumentObject) result.get(0);
        assertThat(getItemsIsbn(first), equalTo("123254545"));
    }

    @Test
    void testKeyAndNonKeyQuery() throws URISyntaxException {
        final String publisher = "jb books";
        final String name = "bad book 1";
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForNameAndPublisher(name, publisher);
        final List<DocumentNode> result = runQueryAndExtractDocuments(documentQuery);
        assertThat(result.size(), equalTo(1));
        final DocumentObject first = (DocumentObject) result.get(0);
        final DocumentStringValue resultsPublisher = (DocumentStringValue) first.get("publisher");
        final DocumentStringValue resultsName = (DocumentStringValue) first.get("name");
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
        final List<DocumentNode> result = runQueryAndExtractDocuments(documentQuery);
        final DocumentObject first = (DocumentObject) result.get(0);
        final DocumentStringValue resultsPublisher = (DocumentStringValue) first.get("publisher");
        final List<String> resultsNames = result.stream()
                .map(each -> ((DocumentStringValue) ((DocumentObject) each).get("name")).getValue())
                .collect(Collectors.toList());
        assertAll(//
                () -> assertThat(resultsPublisher.getValue(), equalTo(publisher)),
                () -> assertThat(resultsNames, containsInAnyOrder(name1, name2))//
        );
    }

    @Test
    void testRangeQuery() throws URISyntaxException {
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForMinPrice(16);
        final List<DocumentNode> result = runQueryAndExtractDocuments(documentQuery);
        assertThat(result.size(), equalTo(1));
        final DocumentObject first = (DocumentObject) result.get(0);
        assertThat(getItemsIsbn(first), equalTo("1235673"));
    }

    @Test
    void testQueryOnIndexAndPrimaryKeyProperties() throws URISyntaxException {
        final String isbn = "123567";
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForPriceAndPublisherAndIsbn(15, "jb books",
                isbn);
        final List<DocumentNode> result = runQueryAndExtractDocuments(documentQuery);
        assertThat(result.size(), equalTo(1));
        final DocumentObject first = (DocumentObject) result.get(0);
        assertThat(getItemsIsbn(first), equalTo(isbn));
    }

    private String getItemsIsbn(final DocumentObject first) {
        return ((DocumentStringValue) first.get("isbn")).getValue();
    }
}