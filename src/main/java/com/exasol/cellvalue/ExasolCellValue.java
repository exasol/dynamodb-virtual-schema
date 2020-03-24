package com.exasol.cellvalue;

/**
 * Interface for handling Exasol values.
 */
// TODO rename to ExasolCellValue
public interface ExasolCellValue {
	/**
	 * Gives the injection free literal representation of the value.
	 * 
	 * @return injection free literal string representation
	 */
	String toLiteral();
}
