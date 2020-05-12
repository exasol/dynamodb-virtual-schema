package com.exasol.adapter.dynamodb.remotetablequery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.literalconverter.SqlLiteralToDocumentValueConverter;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingToSchemaMetadataConverter;
import com.exasol.adapter.dynamodb.mapping.TableMapping;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;

/**
 * Visitor for {@link com.exasol.adapter.sql.SqlStatementSelect} building a {@link RemoteTableQuery}
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class RemoteTableQueryFactory<DocumentVisitorType> {
    private final QueryPredicateFactory<DocumentVisitorType> predicateFactory;

    public RemoteTableQueryFactory(final SqlLiteralToDocumentValueConverter<DocumentVisitorType> literalConverter) {
        this.predicateFactory = new QueryPredicateFactory<>(literalConverter);
    }

    /**
     * Builds the {@link RemoteTableQuery} from an {@link SqlStatementSelect}
     * 
     * @param selectStatement select statement
     * @param schemaMetadata  metadata of the schema
     * @return {@link RemoteTableQuery}
     */
    public RemoteTableQuery<DocumentVisitorType> build(final SqlStatement selectStatement,
            final SchemaMetadata schemaMetadata) throws AdapterException {
        final Visitor visitor = new Visitor();
        selectStatement.accept(visitor);
        final SchemaMappingToSchemaMetadataConverter converter = new SchemaMappingToSchemaMetadataConverter();
        final TableMapping tableMapping = converter.convertBackTable(visitor.tableMetadata, schemaMetadata);
        final QueryPredicate<DocumentVisitorType> selection = this.predicateFactory
                .buildPredicateFor(visitor.getWhereClause());
        return new RemoteTableQuery<>(tableMapping, Collections.unmodifiableList(visitor.resultColumns), selection);
    }

    private static class Visitor extends VoidSqlNodeVisitor {
        private final List<ColumnMapping> resultColumns = new ArrayList<>();
        private String tableName;
        private TableMetadata tableMetadata;
        private SqlNode whereClause;

        @Override
        public Void visit(final SqlStatementSelect select) throws AdapterException {
            select.getFromClause().accept(this);
            select.getSelectList().accept(this);
            this.whereClause = select.getWhereClause();
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
                final ColumnMapping columnMapping = new SchemaMappingToSchemaMetadataConverter()
                        .convertBackColumn(columnMetadata);
                this.resultColumns.add(columnMapping);
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

        private SqlNode getWhereClause() {
            return this.whereClause;
        }
    }
}
