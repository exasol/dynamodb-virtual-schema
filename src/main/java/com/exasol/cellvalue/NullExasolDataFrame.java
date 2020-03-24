package com.exasol.cellvalue;

/**
 * {@link ExasolCellValue} for {@code null} values
 */
public class NullExasolDataFrame implements ExasolCellValue {
	@Override
	public String toLiteral() {
		return "NULL";
	}
}
