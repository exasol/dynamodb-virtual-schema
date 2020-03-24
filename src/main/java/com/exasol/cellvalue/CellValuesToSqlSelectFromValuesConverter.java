package com.exasol.cellvalue;

import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.queryresult.QueryResultTable;

/**
 * This implementation of {@link CellValuesToSqlConverter} converts a DynamoDB
 * result into an {@code SELECT FROM VALUES} statement.
 */
public class CellValuesToSqlSelectFromValuesConverter implements CellValuesToSqlConverter {

	@Override
	public String convert(final QueryResultTable tableStructure, final List<List<ExasolCellValue>> rows) {
		if (rows.isEmpty()) {
			final List<ExasolCellValue> rowOfNullValues = tableStructure.getColumns().stream()
					.map(row -> new NullExasolDataFrame()).collect(Collectors.toList());
			final String emptyRowString = this.convertRow(rowOfNullValues);
			return "SELECT * FROM VALUES" + emptyRowString + " WHERE 0 = 1;";
		}
		final String[] rowsStrings = rows.stream().map(this::convertRow).toArray(String[]::new);
		return "SELECT * FROM (VALUES" + String.join(", ", rowsStrings) + ");";
	}

	private String convertRow(final List<ExasolCellValue> row) {
		final String[] literals = row.stream().map(ExasolCellValue::toLiteral).toArray(String[]::new);
		return "(" + String.join(", ", literals) + ")";
	}
}
