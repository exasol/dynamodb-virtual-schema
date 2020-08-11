package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.io.IOException;
import java.util.List;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.*;
import com.exasol.adapter.dynamodb.queryplanning.RemoteTableQuery;
import com.exasol.adapter.dynamodb.querypredicate.AbstractComparisonPredicate;
import com.exasol.adapter.dynamodb.querypredicate.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.dynamodb.querypredicate.NoPredicate;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.sql.*;

class TestSetup {
    static final String TABLE_NAME = "testTable";
    static final String COLUMN1_NAME = "column1";
    static final ColumnMapping COLUMN1_MAPPING = columnForAttribute(COLUMN1_NAME);
    static final String COLUMN2_NAME = "column2";
    static final ColumnMapping COLUMN2_MAPPING = columnForAttribute(COLUMN2_NAME);
    static final TableMapping TABLE_MAPPING = TableMapping.rootTableBuilder("", TABLE_NAME)
            .withColumnMappingDefinition(COLUMN1_MAPPING).withColumnMappingDefinition(COLUMN2_MAPPING).build();
    static final RemoteTableQuery QUERY_RESULT_TABLE_SCHEMA = new RemoteTableQuery(TABLE_MAPPING,
            List.of(COLUMN1_MAPPING), new NoPredicate(), new NoPredicate());
    final ColumnMetadata column1Metadata = new SchemaMappingToSchemaMetadataConverter().convertColumn(COLUMN1_MAPPING);
    final ColumnMetadata column2Metadata = new SchemaMappingToSchemaMetadataConverter().convertColumn(COLUMN2_MAPPING);

    TestSetup() throws IOException {
    }

    static ColumnMapping columnForAttribute(final String attributeName) {
        final DocumentPathExpression path = DocumentPathExpression.builder().addObjectLookup(attributeName).build();
        return new ToJsonPropertyToColumnMapping(attributeName, path, null, 0, null);
    }

    public static ColumnLiteralComparisonPredicate getCompareForColumn(final String propertyName) {
        final DocumentPathExpression sourcePath = DocumentPathExpression.builder().addObjectLookup(propertyName)
                .build();
        final ToJsonPropertyToColumnMapping column = new ToJsonPropertyToColumnMapping("columnName", sourcePath, null,
                0, MappingErrorBehaviour.NULL);
        return new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.EQUAL, column,
                new SqlLiteralString(""));
    }

    SqlStatement getSelectWithWhereClause(final SqlNode sqlNode) {
        return SqlStatementSelect.builder().fromClause(new SqlTable(TABLE_NAME, null))
                .selectList(SqlSelectList.createSelectStarSelectList()).whereClause(sqlNode).build();
    }
}
