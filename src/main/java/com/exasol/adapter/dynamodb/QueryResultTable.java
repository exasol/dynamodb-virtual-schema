package com.exasol.adapter.dynamodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.exasol_dataframe.ExasolDataFrame;

public class QueryResultTable {
	private final List<QueryResultColumn> columns;

	/**
	 * Constructor.
	 * 
	 * @param columns
	 *            in correct order
	 */
	public QueryResultTable(final List<QueryResultColumn> columns) {
		this.columns = columns;
	}

	public List<QueryResultColumn> getColumns() {
		return this.columns;
	}

	/**
	 * Processes a row according to the given schema definition and gives an exasol
	 * result row
	 * 
	 * @param dynamodbRow
	 */
	public List<ExasolDataFrame> handleRow(final Map<String, AttributeValue> dynamodbRow) throws AdapterException {
		final List<ExasolDataFrame> resultValues = new ArrayList<>(this.columns.size());
		for (final QueryResultColumn resultColumn : this.columns) {
			final ExasolDataFrame result = resultColumn.convertRow(dynamodbRow);
			resultValues.add(result);
		}
		return resultValues;
	}
}
