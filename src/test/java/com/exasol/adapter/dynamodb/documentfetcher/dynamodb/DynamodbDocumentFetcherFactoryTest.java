package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.DynamodbTestInterface;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbPrimaryIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbSecondaryIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;

class DynamodbDocumentFetcherFactoryTest {
    private static final DynamodbTableMetadata tableMetadata = new DynamodbTableMetadata(
            new DynamodbPrimaryIndex(BasicMappingSetup.PRIMARY_KEY_NAME, Optional.empty()), List.of(),
            List.of(new DynamodbSecondaryIndex(BasicMappingSetup.INDEX_PARTITION_KEY,
                    Optional.of(BasicMappingSetup.INDEX_SORT_KEY), BasicMappingSetup.INDEX_NAME)));
    private static BasicMappingSetup basicMappingSetup;

    @BeforeAll
    static void beforeAll() throws DynamodbTestInterface.NoNetworkFoundException, IOException, AdapterException {
        basicMappingSetup = new BasicMappingSetup();
    }

    @Test
    void testSelectAll() {
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup.getSelectAllQuery();
        final DynamodbScanDocumentFetcher scanPlan = (DynamodbScanDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata);
        assertThat(scanPlan.getScanRequest().getTableName(), equalTo(basicMappingSetup.tableMapping.getRemoteName()));
    }

    @Test
    void testSecondaryIndexQuery() {
        final String publisher = "jb books";
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup.getQueryForPublisher(publisher);
        final DynamodbQueryDocumentFetcher queryPlan = (DynamodbQueryDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata);
        assertAll(//
                () -> assertThat(queryPlan.getQueryRequest().getTableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(queryPlan.getQueryRequest().getIndexName(), equalTo("publisherIndex")),
                () -> assertThat(queryPlan.getQueryRequest().getKeyConditionExpression(), equalTo("#0 = :0")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeNames().get("#0"),
                        equalTo("publisher")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeValues().get(":0").getS(),
                        equalTo(publisher))//
        );
    }

    @Test
    void testRangeQueryWithPrimaryKey() {
        final String publisher = "jb books";
        final double price = 10.1;
        final DynamodbTableMetadata tableMetadata = new DynamodbTableMetadata(
                new DynamodbPrimaryIndex("publisher", Optional.of("price")), List.of(), List.of());

        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup
                .getQueryForMinPriceAndPublisher(price, publisher);
        final DynamodbQueryDocumentFetcher queryPlan = (DynamodbQueryDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata);
        assertAll(//
                () -> assertThat(queryPlan.getQueryRequest().getTableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(queryPlan.getQueryRequest().getKeyConditionExpression(),
                        equalTo("#0 = :0 and #1 > :1")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeValues().get(":1").getN(),
                        equalTo(String.valueOf(price))),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeNames().get("#1"), equalTo("price")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeNames().get("#0"),
                        equalTo("publisher")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeValues().get(":0").getS(),
                        equalTo(publisher))//
        );
    }

    @Test
    void testRangeQueryWithPrimaryKeyAndNot() {
        final String publisher = "jb books";
        final double price = 10.1;
        final DynamodbTableMetadata tableMetadata = new DynamodbTableMetadata(
                new DynamodbPrimaryIndex("publisher", Optional.of("price")), List.of(), List.of());

        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup
                .getQueryForMaxPriceAndPublisher(price, publisher);
        final DynamodbQueryDocumentFetcher queryPlan = (DynamodbQueryDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata);
        assertAll(//
                () -> assertThat(queryPlan.getQueryRequest().getTableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(queryPlan.getQueryRequest().getKeyConditionExpression(),
                        equalTo("#0 = :0 and #1 <= :1")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeValues().get(":1").getN(),
                        equalTo(String.valueOf(price))),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeNames().get("#1"), equalTo("price")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeNames().get("#0"),
                        equalTo("publisher")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeValues().get(":0").getS(),
                        equalTo(publisher))//
        );
    }

    @Test
    void testRangeQueryWithoutPrimaryKey() {
        final double price = 10.1;
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup.getQueryForMinPrice(price);
        final DynamodbScanDocumentFetcher scanPlan = (DynamodbScanDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata);
        assertAll(//
                () -> assertThat(scanPlan.getScanRequest().getTableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(scanPlan.getScanRequest().getFilterExpression(), equalTo("#0 > :0")),
                () -> assertThat(scanPlan.getScanRequest().getExpressionAttributeNames().get("#0"), equalTo("price")),
                () -> assertThat(scanPlan.getScanRequest().getExpressionAttributeValues().get(":0").getN(),
                        equalTo(String.valueOf(price))));
    }

    @Test
    void testQueryOnKeyAndNonKeyProperties() {
        final String publisher = "jb books";
        final String name = "test";
        final DynamodbTableMetadata tableMetadata = new DynamodbTableMetadata(
                new DynamodbPrimaryIndex("publisher", Optional.empty()), List.of(), List.of());

        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup.getQueryForNameAndPublisher(name,
                publisher);
        final DynamodbQueryDocumentFetcher queryPlan = (DynamodbQueryDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata);
        assertAll(//
                () -> assertThat(queryPlan.getQueryRequest().getTableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(queryPlan.getQueryRequest().getKeyConditionExpression(), equalTo("#0 = :0")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeValues().get(":0").getS(),
                        equalTo(publisher)),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeNames().get("#0"),
                        equalTo("publisher")),
                () -> assertThat(queryPlan.getQueryRequest().getFilterExpression(), equalTo("#1 = :1")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeNames().get("#1"), equalTo("name")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeValues().get(":1").getS(),
                        equalTo(name))//
        );
    }

    @Test
    void testExtractPrimaryKeyFromTwoPredicates() {
        final String publisher = "jb books";
        final String name1 = "name1";
        final String name2 = "name2";
        final DynamodbTableMetadata tableMetadata = new DynamodbTableMetadata(
                new DynamodbPrimaryIndex("publisher", Optional.empty()), List.of(), List.of());

        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup
                .getQueryForTwoNamesAndPublisher(name1, name2, publisher);
        final DynamodbQueryDocumentFetcher queryPlan = (DynamodbQueryDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata);
        assertAll(//
                () -> assertThat(queryPlan.getQueryRequest().getTableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(queryPlan.getQueryRequest().getKeyConditionExpression(), equalTo("#0 = :0")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeValues().get(":0").getS(),
                        equalTo(publisher)),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeNames().get("#0"),
                        equalTo("publisher")),
                () -> assertThat(queryPlan.getQueryRequest().getFilterExpression(), equalTo("#1 = :1 or #1 = :2")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeNames().get("#1"), equalTo("name")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeValues().get(":1").getS(),
                        equalTo(name1)),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeValues().get(":2").getS(),
                        equalTo(name2))//
        );
    }

    @Test
    void testQueryOnKeyAndIndexProperties() {
        final String price = "10";
        final String publisher = "jb books";
        final String isbn = "1234";
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = basicMappingSetup
                .getQueryForPriceAndPublisherAndIsbn(price, publisher, isbn);
        final DynamodbQueryDocumentFetcher queryPlan = (DynamodbQueryDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata);
        assertAll(//
                () -> assertThat(queryPlan.getQueryRequest().getTableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(queryPlan.getQueryRequest().getIndexName(), equalTo("publisherIndex")),
                () -> assertThat(queryPlan.getQueryRequest().getKeyConditionExpression(),
                        equalTo("#0 = :0 and #1 = :1")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeNames().get("#0"),
                        equalTo("publisher")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeNames().get("#1"), equalTo("price")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeValues().get(":0").getS(),
                        equalTo(publisher)),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeValues().get(":1").getN(),
                        equalTo(price)),
                () -> assertThat(queryPlan.getQueryRequest().getFilterExpression(), equalTo("#2 = :2")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeNames().get("#2"), equalTo("isbn")),
                () -> assertThat(queryPlan.getQueryRequest().getExpressionAttributeValues().get(":2").getS(),
                        equalTo(isbn))//
        );
    }
}