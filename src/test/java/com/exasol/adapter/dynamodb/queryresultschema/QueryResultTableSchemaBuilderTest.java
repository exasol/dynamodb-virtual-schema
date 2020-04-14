package com.exasol.adapter.dynamodb.queryresultschema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.HardCodedMappingFactory;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinitionToSchemaMetadataConverter;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.adapter.sql.SqlTable;

/**
 * Tests for {@link QueryResultTableSchemaBuilder}
 */
public class QueryResultTableSchemaBuilderTest {
    @Test
    void testBuildSelectStar() throws IOException, AdapterException {
        final TableMetadata tableMetadata = new SchemaMappingDefinitionToSchemaMetadataConverter()
                .convert(new HardCodedMappingFactory().getSchemaMapping()).getTables().get(0);
        final SqlStatementSelect statement = SqlStatementSelect.builder()
                .fromClause(new SqlTable(tableMetadata.getName(), tableMetadata))
                .selectList(SqlSelectList.createSelectStarSelectList()).build();
        final QueryResultTableSchema resultTable = new QueryResultTableSchemaBuilder().build(statement);
        final List<String> actualDestinationNames = resultTable.getColumns().stream()
                .map(AbstractColumnMappingDefinition::getExasolColumnName).collect(Collectors.toList());
        final String[] expectedDestinationNames = tableMetadata.getColumns().stream().map(ColumnMetadata::getName)
                .toArray(String[]::new);
        assertThat(actualDestinationNames, containsInAnyOrder(expectedDestinationNames));
    }
}
