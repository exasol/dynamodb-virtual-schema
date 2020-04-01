package com.exasol.adapter.dynamodb.queryresult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinitionToSchemaMetadataConverter;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;

/**
 * Visitor for {@link com.exasol.adapter.sql.SqlStatementSelect} building a
 * {@link QueryResultTable}
 */
public class QueryResultTableBuilder {

	/**
	 * Builds the {@link QueryResultTable} from an {@link SqlStatementSelect}
	 * 
	 * @param selectStatement
	 *            select statement
	 * @return {@link QueryResultTable}
	 */
	public QueryResultTable build(final SqlStatement selectStatement) throws AdapterException {
		final Visitor visitor = new Visitor();
		selectStatement.accept(visitor);
		return visitor.getQueryResultTable();
	}

	private static class Visitor extends VoidSqlNodeVisitor {
		private final List<QueryResultColumn> resultColumns = new ArrayList<>();
		private String tableName;
		private TableMetadata tableMetadata;

		private QueryResultTable getQueryResultTable() {
			return new QueryResultTable(Collections.unmodifiableList(this.resultColumns));
		}

		@Override
		public Void visit(final SqlStatementSelect select) throws AdapterException {
			select.getFromClause().accept(this);
			select.getSelectList().accept(this);
			return null;
		}

		@Override
		public Void visit(final SqlSelectList selectList) throws AdapterException {
			if (selectList.isRequestAnyColumn()) {
				throw new UnsupportedOperationException("not yet implemented");
			} else if (selectList.isSelectStar()) {
				selectAllColumns();
			} else {
				throw new UnsupportedOperationException("not yet implemented");
			}
			return null;
		}

		private void selectAllColumns() throws AdapterException {
			try {
				for (final ColumnMetadata columnMetadata : this.tableMetadata.getColumns()) {
					final AbstractColumnMappingDefinition columnMappingDefinition = SchemaMappingDefinitionToSchemaMetadataConverter
							.convertBackColumn(columnMetadata);
					final QueryResultColumn resultColumn = new QueryResultColumn(columnMappingDefinition);
					this.resultColumns.add(resultColumn);
				}
			} catch (final IOException | ClassNotFoundException exception) {
				throw new AdapterException(
						"Parsing query failed. Cause: could not parse schema. Cause by " + exception.getMessage(),
						exception);
			}
		}

		@Override
		public Void visit(final SqlTable sqlTable) {
			if (this.tableName != null) {
				throw new UnsupportedOperationException("until now only one table can be queried per statement");
			}
			this.tableName = sqlTable.getName();
			this.tableMetadata = sqlTable.getMetadata();
			return null;
		}
	}
}
