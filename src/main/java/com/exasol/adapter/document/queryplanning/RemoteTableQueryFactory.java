package com.exasol.adapter.document.queryplanning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.document.mapping.ColumnMapping;
import com.exasol.adapter.document.mapping.SchemaMappingToSchemaMetadataConverter;
import com.exasol.adapter.document.mapping.TableMapping;
import com.exasol.adapter.document.querypredicate.QueryPredicate;
import com.exasol.adapter.document.querypredicate.QueryPredicateFactory;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;

/**
 * Visitor for {@link com.exasol.adapter.sql.SqlStatementSelect} building a {@link RemoteTableQuery}
 */
public class RemoteTableQueryFactory {
    private final IndexColumnSelectionExtractor indexColumnSelectionExtractor;

    public RemoteTableQueryFactory() {
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
        final QueryPredicate selection = QueryPredicateFactory.getInstance()
                .buildPredicateFor(visitor.getWhereClause());
        final AbstractSelectionExtractor.Result indexColumnExtractionResult = this.indexColumnSelectionExtractor
                .extractIndexColumnSelection(selection);
        return new RemoteTableQuery(tableMapping, Collections.unmodifiableList(visitor.resultColumns),
                indexColumnExtractionResult.getRemainingSelection().asQueryPredicate(),
                indexColumnExtractionResult.getSelectedSelection().asQueryPredicate());
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
                selectAnyColumn();
            } else if (selectList.isSelectStar()) {
                selectAllColumns();
            } else {
                for (final SqlNode selectListExpression : selectList.getExpressions()) {
                    if (!(selectListExpression instanceof SqlColumn)) {
                        throw new UnsupportedOperationException(
                                "The current version of Document Virtual Schema does not support SQL functions.");
                    }
                    final SqlColumn column = (SqlColumn) selectListExpression;
                    addColumnToSelectList(column.getMetadata());
                }
            }
            return null;
        }

        private void selectAnyColumn() {
            final List<ColumnMetadata> columns = this.tableMetadata.getColumns();
            if (columns.isEmpty()) {
                throw new UnsupportedOperationException(
                        "Selecting any column is not possible on tables without columns");
            }
            addColumnToSelectList(columns.get(0));
        }

        private void selectAllColumns() {
            for (final ColumnMetadata columnMetadata : this.tableMetadata.getColumns()) {
                addColumnToSelectList(columnMetadata);
            }
        }

        private void addColumnToSelectList(final ColumnMetadata columnMetadata) {
            final ColumnMapping columnMapping = new SchemaMappingToSchemaMetadataConverter()
                    .convertBackColumn(columnMetadata);
            this.resultColumns.add(columnMapping);
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
