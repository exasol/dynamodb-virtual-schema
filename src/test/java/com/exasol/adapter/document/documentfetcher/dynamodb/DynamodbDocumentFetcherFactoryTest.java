package com.exasol.adapter.document.documentfetcher.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.document.dynamodbmetadata.DynamodbPrimaryIndex;
import com.exasol.adapter.document.dynamodbmetadata.DynamodbSecondaryIndex;
import com.exasol.adapter.document.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.document.queryplanning.RemoteTableQuery;
import com.exasol.adapter.document.querypredicate.QueryPredicate;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamodbDocumentFetcherFactoryTest {
    private static final DynamodbTableMetadata tableMetadata = new DynamodbTableMetadata(
            new DynamodbPrimaryIndex(BasicMappingSetup.PRIMARY_KEY_NAME, Optional.empty()), List.of(),
            List.of(new DynamodbSecondaryIndex(BasicMappingSetup.INDEX_PARTITION_KEY,
                    Optional.of(BasicMappingSetup.INDEX_SORT_KEY), BasicMappingSetup.INDEX_NAME)));
    private static BasicMappingSetup basicMappingSetup;
    @TempDir
    static Path tempDir;

    @BeforeAll
    static void beforeAll() throws IOException, AdapterException {
        basicMappingSetup = new BasicMappingSetup(tempDir);
    }

    @Test
    void testSelectAll() {
        final RemoteTableQuery documentQuery = basicMappingSetup.getSelectAllQuery();
        final DynamodbScanDocumentFetcher scanPlan = (DynamodbScanDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata, 1).getDocumentFetchers().get(0);
        assertThat(scanPlan.getScanRequest().tableName(), equalTo(basicMappingSetup.tableMapping.getRemoteName()));
    }

    @Test
    void testSelectAllWithProjection() {
        final RemoteTableQuery documentQuery = basicMappingSetup.getSelectAllQueryWithNameColumnProjected();
        final DynamodbScanDocumentFetcher scanPlan = (DynamodbScanDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata, 1).getDocumentFetchers().get(0);
        assertAll(//
                () -> assertThat(scanPlan.getScanRequest().tableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(scanPlan.getScanRequest().projectionExpression(), equalTo("#0")),
                () -> assertThat(scanPlan.getScanRequest().expressionAttributeNames(), equalTo(Map.of("#0", "name")))//
        );
    }

    @Test
    void testSecondaryIndexQuery() {
        final String publisher = "jb books";
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForPublisher(publisher);
        final DynamodbQueryDocumentFetcher queryPlan = (DynamodbQueryDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata, 1).getDocumentFetchers().get(0);
        assertAll(//
                () -> assertThat(queryPlan.getQueryRequest().tableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(queryPlan.getQueryRequest().indexName(), equalTo("publisherIndex")),
                () -> assertThat(queryPlan.getQueryRequest().keyConditionExpression(), equalTo("#0 = :0")),
                () -> assertThat(queryPlan.getQueryRequest().expressionAttributeNames().get("#0"),
                        equalTo("publisher")),
                () -> assertThat(queryPlan.getQueryRequest().expressionAttributeValues().get(":0").s(),
                        equalTo(publisher))//
        );
    }

    @Test
    void testRangeQueryWithPrimaryKey() {
        final String publisher = "jb books";
        final double price = 10.1;
        final DynamodbTableMetadata tableMetadata = new DynamodbTableMetadata(
                new DynamodbPrimaryIndex("publisher", Optional.of("price")), List.of(), List.of());

        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForMinPriceAndPublisher(price, publisher);
        final DynamodbQueryDocumentFetcher queryPlan = (DynamodbQueryDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata, 1).getDocumentFetchers().get(0);
        final Map<String, String> attributeNames = queryPlan.getQueryRequest().expressionAttributeNames();
        final Map<String, AttributeValue> attributeValues = queryPlan.getQueryRequest().expressionAttributeValues();
        final String keyConditionExpression = replaceBackPlaceholders(
                queryPlan.getQueryRequest().keyConditionExpression(), attributeNames, attributeValues);
        assertAll(//
                () -> assertThat(queryPlan.getQueryRequest().tableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(keyConditionExpression, anyOf(equalTo("(publisher = 'jb books' and (price > 10.1))"),
                        equalTo("(price > 10.1 and (publisher = 'jb books'))")))//
        );
    }

    @Test
    void testRangeQueryWithPrimaryKeyAndNot() {
        final String publisher = "jb books";
        final double price = 10.1;
        final DynamodbTableMetadata tableMetadata = new DynamodbTableMetadata(
                new DynamodbPrimaryIndex("publisher", Optional.of("price")), List.of(), List.of());

        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForMaxPriceAndPublisher(price, publisher);
        final DynamodbQueryDocumentFetcher queryPlan = (DynamodbQueryDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata, 1).getDocumentFetchers().get(0);
        final Map<String, String> attributeNames = queryPlan.getQueryRequest().expressionAttributeNames();
        final Map<String, AttributeValue> attributeValues = queryPlan.getQueryRequest().expressionAttributeValues();
        final String keyConditionExpression = replaceBackPlaceholders(
                queryPlan.getQueryRequest().keyConditionExpression(), attributeNames, attributeValues);
        assertAll(//
                () -> assertThat(queryPlan.getQueryRequest().tableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(keyConditionExpression, anyOf(equalTo("(publisher = 'jb books' and (price <= 10.1))"),
                        equalTo("(price <= 10.1 and (publisher = 'jb books'))")))//
        );
    }

    @Test
    void testRangeQueryWithoutPrimaryKey() {
        final double price = 10.1;
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForMinPrice(price);
        final DynamodbScanDocumentFetcher scanPlan = (DynamodbScanDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata, 1).getDocumentFetchers().get(0);
        assertAll(//
                () -> assertThat(scanPlan.getScanRequest().tableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(scanPlan.getScanRequest().filterExpression(), equalTo("#0 > :0")),
                () -> assertThat(scanPlan.getScanRequest().expressionAttributeNames().get("#0"), equalTo("price")),
                () -> assertThat(scanPlan.getScanRequest().expressionAttributeValues().get(":0").n(),
                        equalTo(String.valueOf(price))));
    }

    @Test
    void testQueryOnKeyAndNonKeyProperties() {
        final String publisher = "jb books";
        final String name = "test";
        final DynamodbTableMetadata tableMetadata = new DynamodbTableMetadata(
                new DynamodbPrimaryIndex("publisher", Optional.empty()), List.of(), List.of());

        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForNameAndPublisher(name, publisher);
        final DynamodbQueryDocumentFetcher queryPlan = (DynamodbQueryDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata, 1).getDocumentFetchers().get(0);
        assertAll(//
                () -> assertThat(queryPlan.getQueryRequest().tableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(queryPlan.getQueryRequest().keyConditionExpression(), equalTo("#0 = :0")),
                () -> assertThat(queryPlan.getQueryRequest().expressionAttributeValues().get(":0").s(),
                        equalTo(publisher)),
                () -> assertThat(queryPlan.getQueryRequest().expressionAttributeNames().get("#0"),
                        equalTo("publisher")),
                () -> assertThat(queryPlan.getQueryRequest().filterExpression(), equalTo("#1 = :1")),
                () -> assertThat(queryPlan.getQueryRequest().expressionAttributeNames().get("#1"), equalTo("name")),
                () -> assertThat(queryPlan.getQueryRequest().expressionAttributeValues().get(":1").s(), equalTo(name))//
        );
    }

    @Test
    void testExtractPrimaryKeyFromTwoPredicates() {
        final String publisher = "jb books";
        final String name1 = "name1";
        final String name2 = "name2";
        final DynamodbTableMetadata tableMetadata = new DynamodbTableMetadata(
                new DynamodbPrimaryIndex("publisher", Optional.empty()), List.of(), List.of());

        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForTwoNamesAndPublisher(name1, name2,
                publisher);
        final DynamodbQueryDocumentFetcher queryPlan = (DynamodbQueryDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata, 1).getDocumentFetchers().get(0);
        final Map<String, String> attributeNames = queryPlan.getQueryRequest().expressionAttributeNames();
        final Map<String, AttributeValue> attributeValues = queryPlan.getQueryRequest().expressionAttributeValues();
        final String filterExpression = replaceBackPlaceholders(queryPlan.getQueryRequest().filterExpression(),
                attributeNames, attributeValues);
        assertAll(//
                () -> assertThat(queryPlan.getQueryRequest().tableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(queryPlan.getQueryRequest().keyConditionExpression(), equalTo("#0 = :0")),
                () -> assertThat(attributeValues.get(":0").s(), equalTo(publisher)),
                () -> assertThat(attributeNames.get("#0"), equalTo("publisher")),
                () -> assertThat(filterExpression, anyOf(equalTo("(name = 'name1' or (name = 'name2'))"),
                        equalTo("(name = 'name2' or (name = 'name1'))"))));
    }

    @Test
    void testQueryOnKeyAndIndexProperties() {
        final int price = 10;
        final String publisher = "jb books";
        final String isbn = "1234";
        final RemoteTableQuery documentQuery = basicMappingSetup.getQueryForPriceAndPublisherAndIsbn(price, publisher,
                isbn);
        final DynamodbQueryDocumentFetcher queryPlan = (DynamodbQueryDocumentFetcher) new DynamodbDocumentFetcherFactory(
                null).buildDocumentFetcherForQuery(documentQuery, tableMetadata, 1).getDocumentFetchers().get(0);
        final Map<String, String> attributeNames = queryPlan.getQueryRequest().expressionAttributeNames();
        final Map<String, AttributeValue> attributeValues = queryPlan.getQueryRequest().expressionAttributeValues();
        final String filterExpression = replaceBackPlaceholders(queryPlan.getQueryRequest().filterExpression(),
                attributeNames, attributeValues);
        final String keyConditionExpression = replaceBackPlaceholders(
                queryPlan.getQueryRequest().keyConditionExpression(), attributeNames, attributeValues);
        assertAll(//
                () -> assertThat(queryPlan.getQueryRequest().tableName(),
                        equalTo(basicMappingSetup.tableMapping.getRemoteName())),
                () -> assertThat(keyConditionExpression, equalTo("isbn = '1234'")),
                () -> assertThat(filterExpression, anyOf(equalTo("(publisher = 'jb books' and (price = 10.0))"),
                        equalTo("(price = 10.0 and (publisher = 'jb books'))")))//
        );
    }

    @Test
    void testPostSelectionIsExtracted() {
        final RemoteTableQuery documentQuery = basicMappingSetup.getFilterSourceReferenceQuery();
        final QueryPredicate postSelection = new DynamodbDocumentFetcherFactory(null)
                .buildDocumentFetcherForQuery(documentQuery, tableMetadata, 1).getPostSelection();
        assertThat(postSelection.toString(), containsString("SOURCE_REFERENCE"));
    }

    private String replaceBackPlaceholders(final String expression, final Map<String, String> names,
            final Map<String, AttributeValue> values) {
        String result = expression;
        for (final Map.Entry<String, String> namePlaceholder : names.entrySet()) {
            result = result.replace(namePlaceholder.getKey(), namePlaceholder.getValue());
        }
        for (final Map.Entry<String, AttributeValue> valuePlaceholder : values.entrySet()) {
            final AttributeValue value = valuePlaceholder.getValue();
            if (value.s() != null) {
                result = result.replace(valuePlaceholder.getKey(), "'" + value.s() + "'");
            } else if (value.n() != null) {
                result = result.replace(valuePlaceholder.getKey(), value.n());
            } else {
                throw new UnsupportedOperationException("not yet implemented");
            }
        }
        return result;
    }
}