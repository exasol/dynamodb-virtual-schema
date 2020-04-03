package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.AdapterException;

/**
 * Exception on failures in column mapping
 */
public class ColumnMappingException extends AdapterException {
	private final AbstractColumnMappingDefinition causingColumn;

	/**
	 * Creates an instance of {@link ColumnMappingException}.
	 *
	 * @param message
	 *            Exception message
	 * @param column
	 *            {@link AbstractColumnMappingDefinition} that caused exception
	 */
	ColumnMappingException(final String message, final AbstractColumnMappingDefinition column) {
		super(message);
		this.causingColumn = column;
	}

	/**
	 * Get the column that caused this exception.
	 *
	 * @return {@link AbstractColumnMappingDefinition} that caused exception
	 */
	public AbstractColumnMappingDefinition getCausingColumn() {
		return this.causingColumn;
	}
}
