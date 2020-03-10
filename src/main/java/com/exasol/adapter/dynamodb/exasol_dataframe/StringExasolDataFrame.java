package com.exasol.adapter.dynamodb.exasol_dataframe;

/**
 * {@link ExasolDataFrame} for {@code null} values. The values a represented as
 * {@code VARCHAR}.
 */
public class StringExasolDataFrame implements ExasolDataFrame {
	private final String value;
	public StringExasolDataFrame(final String value) {
		this.value = value;
	}

	@Override
	public String toLiteral() {
		if (this.value == null) {
			return new NullExasolDataFrame().toLiteral();
		}
		return "'" + this.value.replace("'", "\\'") + "'";// Todo not sure if injection safe
	}
}
