package com.exasol.adapter.dynamodb.queryresult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.sql.expression.ValueExpression;

/**
 * Represents the result of a query. Using {@link #convertRow(Map)} the rows of
 * the result can be converted.
 */
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

	/**
	 * Get the result columns
	 * 
	 * @return result columns
	 */
	public List<QueryResultColumn> getColumns() {
		return this.columns;
	}

	/**
	 * Processes a row according to the given schema definition and gives an Exasol
	 * result row.
	 * 
	 * @param dynamodbRow
	 *            DynamoDB row
	 */
	public List<ValueExpression> convertRow(final Map<String, AttributeValue> dynamodbRow) throws AdapterException {
		final List<ValueExpression> resultValues = new ArrayList<>(this.columns.size());
		for (final QueryResultColumn resultColumn : this.columns) {
			final ValueExpression result = resultColumn.convertRow(dynamodbRow);
			resultValues.add(result);
		}
		return resultValues;
	}
}
