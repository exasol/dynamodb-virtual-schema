package com.exasol.adapter.dynamodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.mapping_definition.ColumnMappingDefinition;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.adapter.sql.SqlTable;

/**
 * Visitor for {@link com.exasol.adapter.sql.SqlStatementSelect} building a
 * {@link QueryResultTable}
 */
public class QueryResultTableBuilder extends GenericSchemaMappingVisitor {
	private final List<QueryResultColumn> resultColumns = new ArrayList<>();
	private String tableName;
	private TableMetadata tableMetadata;
	private boolean wasVisited = false;

	/**
	 * Gives the built {@link QueryResultTable} or null if was not used as visitor
	 * before.
	 * 
	 * @return {@link QueryResultTable}
	 */
	public QueryResultTable getQueryResultTable() {
		if (!this.wasVisited) {
			return null;
		}
		return new QueryResultTable(Collections.unmodifiableList(this.resultColumns));
	}

	@Override
	public Void visit(final SqlStatementSelect select) throws AdapterException {
		select.getFromClause().accept(this);
		select.getSelectList().accept(this);
		this.wasVisited = true;
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
				final ColumnMappingDefinition columnMappingDefinition = ColumnMappingDefinition
						.fromColumnMetadata(columnMetadata);
				final QueryResultColumn resultColumn = new QueryResultColumn(columnMappingDefinition);
				this.resultColumns.add(resultColumn);
			}
		} catch (final IOException | ClassNotFoundException e) {
			throw new AdapterException(
					"Parsing query failed. Cause: could not parse schema. Cause by " + e.getMessage(), e);
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
