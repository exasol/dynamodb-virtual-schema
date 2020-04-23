package com.exasol.adapter.dynamodb.queryplan;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.documentnode.DocumentValue;
import com.exasol.adapter.dynamodb.literalconverter.NotALiteralException;
import com.exasol.adapter.dynamodb.literalconverter.SqlLiteralToDocumentValueConverter;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.HardCodedMappingFactory;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinitionToSchemaMetadataConverter;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.SqlNode;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.adapter.sql.SqlTable;

public class DocumentQueryBuilderTest {
    @Test
    void testBuildSelectStar() throws IOException, AdapterException {
        final SchemaMetadata schemaMetadata = new SchemaMappingDefinitionToSchemaMetadataConverter()
                .convert(new HardCodedMappingFactory().getSchemaMapping());
        final TableMetadata tableMetadata = schemaMetadata.getTables().get(0);
        final SqlStatementSelect statement = SqlStatementSelect.builder()
                .fromClause(new SqlTable(tableMetadata.getName(), tableMetadata))
                .selectList(SqlSelectList.createSelectStarSelectList()).build();
        final DocumentQuery<Object> resultTable = new DocumentQueryFactory<Object>(
                new SqlLiteralToDocumentValueConverterStub()).build(statement, schemaMetadata);
        final List<String> actualDestinationNames = resultTable.getSelectList().stream()
                .map(AbstractColumnMappingDefinition::getExasolColumnName).collect(Collectors.toList());
        final String[] expectedDestinationNames = tableMetadata.getColumns().stream().map(ColumnMetadata::getName)
                .toArray(String[]::new);
        assertThat(actualDestinationNames, containsInAnyOrder(expectedDestinationNames));
    }

    private class SqlLiteralToDocumentValueConverterStub implements SqlLiteralToDocumentValueConverter<Object> {

        @Override
        public DocumentValue<Object> convert(final SqlNode exasolLiteralNode) throws NotALiteralException {
            return new DocumentValueStub();
        }
    }

    private class DocumentValueStub implements DocumentValue<Object> {

        @Override
        public void accept(final Object visitor) {

        }
    }
}
