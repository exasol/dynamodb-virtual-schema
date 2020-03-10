package com.exasol.adapter.dynamodb.exasol_dataframe;

/**
 * Interface for handling Exasol values.
 */
public interface ExasolDataFrame {
	/**
	 * Gives the injection free literal representation of the value.
	 * 
	 * @return injection free literal string representation
	 */
	String toLiteral();
}
