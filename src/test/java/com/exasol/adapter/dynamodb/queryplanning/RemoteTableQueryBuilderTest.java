package com.exasol.adapter.dynamodb.queryplanning;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.HardCodedSchemaMappingReader;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingToSchemaMetadataConverter;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.adapter.sql.SqlTable;

class RemoteTableQueryBuilderTest {
    @Test
    void testBuildSelectStar() throws IOException, AdapterException {
        final SchemaMetadata schemaMetadata = new SchemaMappingToSchemaMetadataConverter()
                .convert(new HardCodedSchemaMappingReader().getSchemaMapping());
        final TableMetadata tableMetadata = schemaMetadata.getTables().get(0);
        final SqlStatementSelect statement = SqlStatementSelect.builder()
                .fromClause(new SqlTable(tableMetadata.getName(), tableMetadata))
                .selectList(SqlSelectList.createSelectStarSelectList()).build();
        final RemoteTableQuery resultTable = new RemoteTableQueryFactory().build(statement,
                schemaMetadata.getAdapterNotes());
        final List<String> actualDestinationNames = resultTable.getSelectList().stream()
                .map(ColumnMapping::getExasolColumnName).collect(Collectors.toList());
        final String[] expectedDestinationNames = tableMetadata.getColumns().stream().map(ColumnMetadata::getName)
                .toArray(String[]::new);
        assertThat(actualDestinationNames, containsInAnyOrder(expectedDestinationNames));
    }
}
