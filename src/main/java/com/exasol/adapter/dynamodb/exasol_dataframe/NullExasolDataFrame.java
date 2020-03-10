package com.exasol.adapter.dynamodb.exasol_dataframe;

/**
 * {@link ExasolDataFrame} for {@code null} values
 */
public class NullExasolDataFrame implements ExasolDataFrame {
	@Override
	public String toLiteral() {
		return "NULL";
	}
}
