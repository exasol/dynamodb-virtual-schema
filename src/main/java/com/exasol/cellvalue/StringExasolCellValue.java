package com.exasol.cellvalue;

/**
 * {@link ExasolCellValue} for {@code null} values. The values a represented as
 * {@code VARCHAR}.
 */
public class StringExasolCellValue implements ExasolCellValue {
	private final String value;
	public StringExasolCellValue(final String value) {
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
