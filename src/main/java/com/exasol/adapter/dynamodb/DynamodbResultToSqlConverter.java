package com.exasol.adapter.dynamodb;

import java.util.List;

import com.exasol.adapter.dynamodb.exasol_dataframe.ExasolDataFrame;

/**
 * Interface for converting a list of {@link ExasolDataFrame} into a SQL
 * Statement for pushdown response.
 */
public interface DynamodbResultToSqlConverter {
	/**
	 * Converts a list of lists of {@link ExasolDataFrame} into an SQL Statement.
	 *
	 * @param tableStructure
	 *            used for creating empty row
	 * @param rows
	 *            List of ordered lists of {@link ExasolDataFrame}
	 * @return SQL Statement
	 */
	String convert(final QueryResultTable tableStructure, final List<List<ExasolDataFrame>> rows);
}
