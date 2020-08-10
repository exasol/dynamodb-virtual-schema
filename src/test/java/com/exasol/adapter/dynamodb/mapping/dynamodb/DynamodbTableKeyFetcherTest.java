package com.exasol.adapter.dynamodb.mapping.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbPrimaryIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadataFactory;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.MappingTestFiles;
import com.exasol.adapter.dynamodb.mapping.TableKeyFetcher;
import com.exasol.adapter.dynamodb.mapping.reader.JsonSchemaMappingReader;

class DynamodbTableKeyFetcherTest {

    private static final String TEST_TABLE_NAME = "testTableName";
    private static List<ColumnMapping> COLUMNS;
    private static ColumnMapping ISBN_COLUMN;
    private static ColumnMapping PUBLISHER_COLUMN;

    @BeforeAll
    static void beforeAll() throws IOException, AdapterException {
        COLUMNS = new JsonSchemaMappingReader(MappingTestFiles.BASIC_MAPPING_FILE, null).getSchemaMapping()
                .getTableMappings().get(0).getColumns();
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
    void testKeyFetchFails() throws TableKeyFetcher.NoKeyFoundException {
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