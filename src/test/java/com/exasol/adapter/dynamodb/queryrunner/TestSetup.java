package com.exasol.adapter.dynamodb.queryrunner;

import java.io.IOException;
import java.util.List;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinitionToSchemaMetadataConverter;
import com.exasol.adapter.dynamodb.mapping.TableMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.dynamodb.remotetablequery.NoPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.sql.*;

class TestSetup {
    static final String TABLE_NAME = "testTable";
    static final String COLUMN1_NAME = "column1";
    static final AbstractColumnMappingDefinition COLUMN1_MAPPING = columnForAttribute(COLUMN1_NAME);
    static final String COLUMN2_NAME = "column2";
    static final AbstractColumnMappingDefinition COLUMN2_MAPPING = columnForAttribute(COLUMN2_NAME);
    static final TableMappingDefinition TABLE_MAPPING = TableMappingDefinition.rootTableBuilder("", TABLE_NAME)
            .withColumnMappingDefinition(COLUMN1_MAPPING).withColumnMappingDefinition(COLUMN2_MAPPING).build();
    static final RemoteTableQuery<DynamodbNodeVisitor> QUERY_RESULT_TABLE_SCHEMA = new RemoteTableQuery<>(TABLE_MAPPING,
            List.of(COLUMN1_MAPPING), new NoPredicate<>());
    final ColumnMetadata column1Metadata = new SchemaMappingDefinitionToSchemaMetadataConverter()
            .convertColumn(COLUMN1_MAPPING);
    final ColumnMetadata column2Metadata = new SchemaMappingDefinitionToSchemaMetadataConverter()
            .convertColumn(COLUMN2_MAPPING);

    TestSetup() throws IOException {
    }

    static AbstractColumnMappingDefinition columnForAttribute(final String attributeName) {
        final DocumentPathExpression path = new DocumentPathExpression.Builder().addObjectLookup(attributeName).build();
        return new ToJsonColumnMappingDefinition(
                new AbstractColumnMappingDefinition.ConstructorParameters(attributeName, path, null));
    }

    SqlStatement getSelectWithWhereClause(final SqlNode sqlNode) {
        return SqlStatementSelect.builder().fromClause(new SqlTable(TABLE_NAME, null))
                .selectList(SqlSelectList.createSelectStarSelectList()).whereClause(sqlNode).build();
    }
}
