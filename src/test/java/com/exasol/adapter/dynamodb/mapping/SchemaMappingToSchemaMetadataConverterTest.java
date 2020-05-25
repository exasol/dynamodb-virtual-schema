package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;

class SchemaMappingToSchemaMetadataConverterTest {
    private static final String DEST_TABLE_NAME = "TEST";
    private static final String SRC_TABLE_NAME = "srcTable";
    private static final String COLUMN_NAME = "json";

    public SchemaMapping getSchemaMapping() {
        final TableMapping table = TableMapping.rootTableBuilder(DEST_TABLE_NAME, SRC_TABLE_NAME)
                .withColumnMappingDefinition(new ToJsonPropertyToColumnMapping(COLUMN_NAME,
                        DocumentPathExpression.empty(), LookupFailBehaviour.DEFAULT_VALUE))
                .build();
        return new SchemaMapping(List.of(table));
    }

    @Test
    void testConvert() throws IOException {
        final SchemaMapping schemaMapping = getSchemaMapping();
        final SchemaMetadata schemaMetadata = new SchemaMappingToSchemaMetadataConverter().convert(schemaMapping);
        final List<TableMetadata> tables = schemaMetadata.getTables();
        final TableMetadata firstTable = tables.get(0);
        final List<String> columnNames = firstTable.getColumns().stream().map(ColumnMetadata::getName)
                .collect(Collectors.toList());
        assertAll(//
                () -> assertThat(tables.size(), equalTo(1)), //
                () -> assertThat(firstTable.getName(), equalTo(DEST_TABLE_NAME)),
                () -> assertThat(columnNames, containsInAnyOrder(COLUMN_NAME))//
        );
    }

    @Test
    void testColumnSerialization() throws IOException {
        final SchemaMapping schemaMapping = getSchemaMapping();
        final SchemaMetadata schemaMetadata = new SchemaMappingToSchemaMetadataConverter().convert(schemaMapping);
        final ColumnMetadata firstColumnMetadata = schemaMetadata.getTables().get(0).getColumns().get(0);
        final ColumnMapping columnMapping = new SchemaMappingToSchemaMetadataConverter()
                .convertBackColumn(firstColumnMetadata);
        assertThat(columnMapping.getExasolColumnName(), equalTo(COLUMN_NAME));
    }

    @Test
    void testTableSerialization() throws IOException {
        final SchemaMapping schemaMapping = getSchemaMapping();
        final SchemaMappingToSchemaMetadataConverter converter = new SchemaMappingToSchemaMetadataConverter();
        final SchemaMetadata schemaMetadata = converter.convert(schemaMapping);
        final TableMetadata firstTableMetadata = schemaMetadata.getTables().get(0);
        final TableMapping tableMapping = converter.convertBackTable(firstTableMetadata,
                schemaMetadata.getAdapterNotes());
        assertAll(//
                () -> assertThat(tableMapping.getExasolName(), equalTo(DEST_TABLE_NAME)), //
                () -> assertThat(tableMapping.getRemoteName(), equalTo(SRC_TABLE_NAME)), //
                () -> assertThat(tableMapping.getColumns().size(), equalTo(1)), //
                () -> assertThat(tableMapping.getColumns().get(0).getExasolColumnName(), equalTo(COLUMN_NAME))//
        );
    }
}
