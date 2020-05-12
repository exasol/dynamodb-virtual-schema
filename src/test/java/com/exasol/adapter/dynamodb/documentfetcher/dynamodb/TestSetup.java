package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.io.IOException;
import java.util.List;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbString;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingToSchemaMetadataConverter;
import com.exasol.adapter.dynamodb.mapping.TableMapping;
import com.exasol.adapter.dynamodb.mapping.ToJsonPropertyToColumnMapping;
import com.exasol.adapter.dynamodb.remotetablequery.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.ComparisonPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.NoPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;
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
    static final RemoteTableQuery<DynamodbNodeVisitor> QUERY_RESULT_TABLE_SCHEMA = new RemoteTableQuery<>(TABLE_MAPPING,
            List.of(COLUMN1_MAPPING), new NoPredicate<>());
    final ColumnMetadata column1Metadata = new SchemaMappingToSchemaMetadataConverter().convertColumn(COLUMN1_MAPPING);
    final ColumnMetadata column2Metadata = new SchemaMappingToSchemaMetadataConverter().convertColumn(COLUMN2_MAPPING);

    TestSetup() throws IOException {
    }

    static ColumnMapping columnForAttribute(final String attributeName) {
        final DocumentPathExpression path = new DocumentPathExpression.Builder().addObjectLookup(attributeName).build();
        return new ToJsonPropertyToColumnMapping(attributeName, path, null);
    }

    public static ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> getCompareForColumn(final String propertyName) {
        final DocumentPathExpression sourcePath = new DocumentPathExpression.Builder().addObjectLookup(propertyName)
                .build();
        final ToJsonPropertyToColumnMapping column = new ToJsonPropertyToColumnMapping("columnName", sourcePath, null);
        return new ColumnLiteralComparisonPredicate<>(ComparisonPredicate.Operator.EQUAL, column,
                new DynamodbString(""));
    }

    SqlStatement getSelectWithWhereClause(final SqlNode sqlNode) {
        return SqlStatementSelect.builder().fromClause(new SqlTable(TABLE_NAME, null))
                .selectList(SqlSelectList.createSelectStarSelectList()).whereClause(sqlNode).build();
    }
}
