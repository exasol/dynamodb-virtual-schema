package com.exasol.adapter.dynamodb.queryplanning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingToSchemaMetadataConverter;
import com.exasol.adapter.dynamodb.mapping.TableMapping;
import com.exasol.adapter.dynamodb.querypredicate.QueryPredicate;
import com.exasol.adapter.dynamodb.querypredicate.QueryPredicateFactory;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;

/**
 * Visitor for {@link com.exasol.adapter.sql.SqlStatementSelect} building a {@link RemoteTableQuery}
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class RemoteTableQueryFactory {
    private final QueryPredicateFactory predicateFactory;
    private final IndexColumnSelectionExtractor indexColumnSelectionExtractor;

    public RemoteTableQueryFactory() {
        this.predicateFactory = new QueryPredicateFactory();
        this.indexColumnSelectionExtractor = new IndexColumnSelectionExtractor();
    }

    /**
     * Builds the {@link RemoteTableQuery} from an {@link SqlStatementSelect}
     * 
     * @param selectStatement    select statement
     * @param schemaAdapterNotes adapter notes of the schema
     * @return {@link RemoteTableQuery}
     */
    public RemoteTableQuery build(final SqlStatement selectStatement, final String schemaAdapterNotes)
            throws AdapterException {
        final Visitor visitor = new Visitor();
        selectStatement.accept(visitor);
        final SchemaMappingToSchemaMetadataConverter converter = new SchemaMappingToSchemaMetadataConverter();
        final TableMapping tableMapping = converter.convertBackTable(visitor.tableMetadata, schemaAdapterNotes);
        final QueryPredicate selection = this.predicateFactory.buildPredicateFor(visitor.getWhereClause());
        final IndexColumnSelectionExtractor.Result indexColumnExtractionResult = this.indexColumnSelectionExtractor
                .extractIndexColumnSelection(selection);
        return new RemoteTableQuery(tableMapping, Collections.unmodifiableList(visitor.resultColumns),
                indexColumnExtractionResult.getNonIndexSelection().asQueryPredicate(),
                indexColumnExtractionResult.getIndexSelection().asQueryPredicate());
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
