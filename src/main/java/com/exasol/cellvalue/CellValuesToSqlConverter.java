package com.exasol.cellvalue;

import java.util.List;

import com.exasol.adapter.dynamodb.queryresult.QueryResultTable;

/**
 * Interface for converting a list of {@link ExasolCellValue} into a SQL
 * Statement for pushdown response.
 */
public interface CellValuesToSqlConverter {
	/**
	 * Converts a list of lists of {@link ExasolCellValue} into an SQL Statement.
	 *
	 * @param tableStructure
	 *            used for creating empty row
	 * @param rows
	 *            List of ordered lists of {@link ExasolCellValue}
	 * @return SQL Statement
	 */
	String convert(final QueryResultTable tableStructure, final List<List<ExasolCellValue>> rows);
}
