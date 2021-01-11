package com.exasol.adapter.document.mapping.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.dynamodbmetadata.DynamodbPrimaryIndex;
import com.exasol.adapter.document.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.document.dynamodbmetadata.DynamodbTableMetadataFactory;
import com.exasol.adapter.document.mapping.ColumnMapping;
import com.exasol.adapter.document.mapping.MappingTestFiles;
import com.exasol.adapter.document.mapping.TableKeyFetcher;
import com.exasol.adapter.document.mapping.reader.JsonSchemaMappingReader;

class DynamodbTableKeyFetcherTest {
    private static final String TEST_TABLE_NAME = "testTableName";
    private static List<ColumnMapping> COLUMNS;
    private static ColumnMapping ISBN_COLUMN;
    private static ColumnMapping PUBLISHER_COLUMN;

    @BeforeAll
    static void beforeAll() {
        COLUMNS = new JsonSchemaMappingReader(new File(
                DynamodbTableKeyFetcher.class.getClassLoader().getResource(MappingTestFiles.BASIC_MAPPING).getFile()),
                null).getSchemaMapping().getTableMappings().get(0).getColumns();
        ISBN_COLUMN = COLUMNS.stream().filter(column -> column.getExasolColumnName().equals("ISBN")).findAny().get();
        PUBLISHER_COLUMN = COLUMNS.stream().filter(column -> column.getExasolColumnName().equals("PUBLISHER")).findAny()
                .get();
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