package com.exasol.adapter.dynamodb.queryresult;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinitionToSchemaMetadataConverter;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinitionToSchemaMetadataConverterTest;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.mapping.HardCodedMappingProvider;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.adapter.sql.SqlTable;

/**
 * Tests for {@link QueryResultTableBuilder}
 */
public class QueryResultTableBuilderTest {
	@Test
	void testBuildSelectStar() throws IOException, AdapterException {
		final TableMetadata tableMetadata = SchemaMappingDefinitionToSchemaMetadataConverter.convert(new HardCodedMappingProvider().getSchemaMapping())
				.getTables().get(0);
		final SqlStatementSelect statement = SqlStatementSelect.builder()
				.fromClause(new SqlTable(tableMetadata.getName(), tableMetadata))
				.selectList(SqlSelectList.createSelectStarSelectList()).build();
		final QueryResultTableBuilder queryResultTableBuilder = new QueryResultTableBuilder();
		queryResultTableBuilder.visit(statement);
		final QueryResultTable resultTable = queryResultTableBuilder.getQueryResultTable();
		final List<String> actualDestinationNames = resultTable.getColumns().stream()
				.map(column -> column.getColumnMapping().getDestinationName()).collect(Collectors.toList());
		final String[] expectedDestinationNames = tableMetadata.getColumns().stream().map(ColumnMetadata::getName)
				.toArray(String[]::new);
		assertThat(actualDestinationNames, containsInAnyOrder(expectedDestinationNames));
	}
}
