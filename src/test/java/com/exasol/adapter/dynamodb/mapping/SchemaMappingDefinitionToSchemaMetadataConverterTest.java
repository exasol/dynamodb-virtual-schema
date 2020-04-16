package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.dynamodb.resultwalker.IdentityDynamodbResultWalker;

public class SchemaMappingDefinitionToSchemaMetadataConverterTest {
    private static final String TABLE_NAME = "testTable";
    private static final String COLUMN_NAME = "json";

    public SchemaMappingDefinition getSchemaMapping() {
        final TableMappingDefinition table = TableMappingDefinition.rootTableBuilder(TABLE_NAME)
                .withColumnMappingDefinition(
                        new ToJsonColumnMappingDefinition(new AbstractColumnMappingDefinition.ConstructorParameters(
                                COLUMN_NAME, new IdentityDynamodbResultWalker(),
                                AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE)))
                .build();
        return new SchemaMappingDefinition(List.of(table));
    }

    @Test
    void testConvert() throws IOException {
        final SchemaMappingDefinition schemaMapping = getSchemaMapping();
        final SchemaMetadata schemaMetadata = new SchemaMappingDefinitionToSchemaMetadataConverter()
                .convert(schemaMapping);
        final List<TableMetadata> tables = schemaMetadata.getTables();
        assertThat(tables.size(), equalTo(1));
        final TableMetadata firstTable = tables.get(0);
        assertThat(firstTable.getName(), equalTo(TABLE_NAME));
        final List<String> columnNames = firstTable.getColumns().stream().map(ColumnMetadata::getName)
                .collect(Collectors.toList());
        assertThat(columnNames, containsInAnyOrder(COLUMN_NAME));
    }

    @Test
    void testColumnSerialization() throws IOException, ClassNotFoundException {
        final SchemaMappingDefinition schemaMapping = getSchemaMapping();
        final SchemaMetadata schemaMetadata = new SchemaMappingDefinitionToSchemaMetadataConverter()
                .convert(schemaMapping);
        final ColumnMetadata firstColumnMetadata = schemaMetadata.getTables().get(0).getColumns().get(0);
        final AbstractColumnMappingDefinition columnMappingDefinition = new SchemaMappingDefinitionToSchemaMetadataConverter()
                .convertBackColumn(firstColumnMetadata);
        assertThat(columnMappingDefinition.getExasolColumnName(), equalTo(COLUMN_NAME));
    }

    @Test
    void testTableSerialization() throws IOException, ClassNotFoundException {
        final SchemaMappingDefinition schemaMapping = getSchemaMapping();
        final SchemaMappingDefinitionToSchemaMetadataConverter converter = new SchemaMappingDefinitionToSchemaMetadataConverter();
        final SchemaMetadata schemaMetadata = converter.convert(schemaMapping);
        final TableMetadata firstTableMetadata = schemaMetadata.getTables().get(0);
        final TableMappingDefinition tableMappingDefinition = converter.convertBackTable(firstTableMetadata,
                schemaMetadata);
        assertAll(//
                () -> assertThat(tableMappingDefinition.getExasolName(), equalTo(TABLE_NAME)), //
                () -> assertThat(tableMappingDefinition.getColumns().size(), equalTo(1)), //
                () -> assertThat(tableMappingDefinition.getColumns().get(0).getExasolColumnName(), equalTo(COLUMN_NAME))//
        );
    }
}
