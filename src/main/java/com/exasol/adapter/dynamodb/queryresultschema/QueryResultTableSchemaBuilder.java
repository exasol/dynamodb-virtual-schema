package com.exasol.adapter.dynamodb.queryresultschema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinitionToSchemaMetadataConverter;
import com.exasol.adapter.dynamodb.mapping.TableMappingDefinition;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;

/**
 * Visitor for {@link com.exasol.adapter.sql.SqlStatementSelect} building a {@link QueryResultTableSchema}
 */
public class QueryResultTableSchemaBuilder {

    /**
     * Builds the {@link QueryResultTableSchema} from an {@link SqlStatementSelect}
     * 
     * @param selectStatement select statement
     * @param schemaMetadata  metadata of the schema
     * @return {@link QueryResultTableSchema}
     */
    public QueryResultTableSchema build(final SqlStatement selectStatement, final SchemaMetadata schemaMetadata)
            throws AdapterException {
        final Visitor visitor = new Visitor();
        selectStatement.accept(visitor);
        return visitor.getQueryResultTable(schemaMetadata);
    }

    private static class Visitor extends VoidSqlNodeVisitor {
        private final List<AbstractColumnMappingDefinition> resultColumns = new ArrayList<>();
        private String tableName;
        private TableMetadata tableMetadata;

        private QueryResultTableSchema getQueryResultTable(final SchemaMetadata schemaMetadata) {
            final SchemaMappingDefinitionToSchemaMetadataConverter converter = new SchemaMappingDefinitionToSchemaMetadataConverter();
            final TableMappingDefinition tableMappingDefinition = converter.convertBackTable(this.tableMetadata,
                    schemaMetadata);
            return new QueryResultTableSchema(tableMappingDefinition, Collections.unmodifiableList(this.resultColumns));
        }

        @Override
        public Void visit(final SqlStatementSelect select) throws AdapterException {
            select.getFromClause().accept(this);
            select.getSelectList().accept(this);
            return null;
        }

        @Override
        public Void visit(final SqlSelectList selectList) {
            if (selectList.isRequestAnyColumn()) {
                throw new UnsupportedOperationException(
                        "The current version of DynamoDB Virtual Schema does not support requesting any columns.");
            } else if (selectList.isSelectStar()) {
                selectAllColumns();
            } else {
                throw new UnsupportedOperationException(
                        "The current version of DynamoDB Virtual Schema does not support projection.");
            }
            return null;
        }

        private void selectAllColumns() {
            for (final ColumnMetadata columnMetadata : this.tableMetadata.getColumns()) {
                final AbstractColumnMappingDefinition columnMappingDefinition = new SchemaMappingDefinitionToSchemaMetadataConverter()
                        .convertBackColumn(columnMetadata);
                this.resultColumns.add(columnMappingDefinition);
            }
        }

        @Override
        public Void visit(final SqlTable sqlTable) {
            if (this.tableName != null) {
                throw new UnsupportedOperationException(
                        "The current version of DynamoDB Virtual Schema does only support one table per statement.");
            }
            this.tableName = sqlTable.getName();
            this.tableMetadata = sqlTable.getMetadata();
            return null;
        }
    }
}
