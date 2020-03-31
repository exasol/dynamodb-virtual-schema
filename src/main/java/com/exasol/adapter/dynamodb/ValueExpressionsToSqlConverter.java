package com.exasol.adapter.dynamodb;

import java.util.List;

import com.exasol.adapter.dynamodb.queryresult.QueryResultTable;
import com.exasol.sql.expression.ValueExpression;

/**
 * Interface for converting a list of {@link ValueExpression} into a SQL
 * Statement for pushdown response.
 */
public interface ValueExpressionsToSqlConverter {
	/**
	 * Converts a list of lists of {@link ValueExpression} into an SQL Statement.
	 *
	 * @param tableStructure
	 *            used for creating empty row
	 * @param rows
	 *            List of ordered lists of {@link ValueExpression}
	 * @return SQL Statement
	 */
	String convert(final QueryResultTable tableStructure, final List<List<ValueExpression>> rows);
}