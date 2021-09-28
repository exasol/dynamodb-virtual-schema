package com.exasol.adapter.document.mapping.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentfetcher.dynamodb.BasicMappingSetup;
import com.exasol.adapter.document.dynamodbmetadata.*;
import com.exasol.adapter.document.mapping.ColumnMapping;
import com.exasol.adapter.document.mapping.TableKeyFetcher;

class DynamodbTableKeyFetcherTest {
    private static final String TEST_TABLE_NAME = "testTableName";
    private static List<ColumnMapping> COLUMNS;
    private static ColumnMapping ISBN_COLUMN;
    private static ColumnMapping PUBLISHER_COLUMN;

    @BeforeAll
    static void beforeAll() throws IOException {
        final BasicMappingSetup basicMappingSetup = new BasicMappingSetup();
        ISBN_COLUMN = basicMappingSetup.getIsbnColumn();
        PUBLISHER_COLUMN = basicMappingSetup.getPublisherColumn();
        COLUMNS = List.of(ISBN_COLUMN, PUBLISHER_COLUMN, basicMappingSetup.getPriceColumn());
    }

    @Test
    void testFetchGlobalKey() throws TableKeyFetcher.NoKeyFoundException {
        final DynamodbTableMetadataFactory metadataFactoryStub = getStubTableMetadataFactoryForPrimaryIndex(
                new DynamodbPrimaryIndex("isbn", Optional.empty()));
        final DynamodbTableKeyFetcher keyFetcher = new DynamodbTableKeyFetcher(metadataFactoryStub);
        final List<ColumnMapping> keyColumns = keyFetcher.fetchKeyForTable(TEST_TABLE_NAME, COLUMNS);
        assertThat(keyColumns, containsInAnyOrder(ISBN_COLUMN));
    }

    @Test
    void testFetchCompoundGlobalKey() throws TableKeyFetcher.NoKeyFoundException {
        final DynamodbTableMetadataFactory metadataFactoryStub = getStubTableMetadataFactoryForPrimaryIndex(
                new DynamodbPrimaryIndex("isbn", Optional.of("publisher")));
        final DynamodbTableKeyFetcher keyFetcher = new DynamodbTableKeyFetcher(metadataFactoryStub);
        final List<ColumnMapping> keyColumns = keyFetcher.fetchKeyForTable(TEST_TABLE_NAME, COLUMNS);
        assertThat(keyColumns, containsInAnyOrder(ISBN_COLUMN, PUBLISHER_COLUMN));
    }

    @Test
    void testKeyFetchFails() {
        final DynamodbTableMetadataFactory metadataFactoryStub = getStubTableMetadataFactoryForPrimaryIndex(
                new DynamodbPrimaryIndex("nonMappedColumn", Optional.empty()));
        final DynamodbTableKeyFetcher keyFetcher = new DynamodbTableKeyFetcher(metadataFactoryStub);
        assertThrows(TableKeyFetcher.NoKeyFoundException.class,
                () -> keyFetcher.fetchKeyForTable(TEST_TABLE_NAME, COLUMNS));
    }

    private DynamodbTableMetadataFactory getStubTableMetadataFactoryForPrimaryIndex(
            final DynamodbPrimaryIndex primaryIndex) {
        return tableName -> {
            assertThat(tableName, equalTo(TEST_TABLE_NAME));
            return new DynamodbTableMetadata(primaryIndex, Collections.emptyList(), Collections.emptyList());
        };
    }
}