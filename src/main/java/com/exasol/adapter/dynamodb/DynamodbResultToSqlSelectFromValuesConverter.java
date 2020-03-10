package com.exasol.adapter.dynamodb;

import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.exasol_dataframe.ExasolDataFrame;
import com.exasol.adapter.dynamodb.exasol_dataframe.NullExasolDataFrame;

/**
 * This implementation of {@link DynamodbResultToSqlConverter} converts a
 * DynamoDB result into an {@code SELECT FROM VALUES} statement.
 */
public class DynamodbResultToSqlSelectFromValuesConverter implements DynamodbResultToSqlConverter {

	@Override
	public String convert(final QueryResultTable tableStructure, final List<List<ExasolDataFrame>> rows) {
		if (rows.isEmpty()) {
			final List<ExasolDataFrame> rowOfNullValues = tableStructure.getColumns().stream()
					.map(row -> new NullExasolDataFrame()).collect(Collectors.toList());
			final String emptyRowString = this.convertRow(rowOfNullValues);
			return "SELECT * FROM VALUES" + emptyRowString + " WHERE 0 = 1;";
		}
		final String[] rowsStrings = rows.stream().map(this::convertRow).toArray(String[]::new);
		return "SELECT * FROM (VALUES" + String.join(", ", rowsStrings) + ");";
	}

	private String convertRow(final List<ExasolDataFrame> row) {
		final String[] literals = row.stream().map(ExasolDataFrame::toLiteral).toArray(String[]::new);
		return "(" + String.join(", ", literals) + ")";
	}
}
