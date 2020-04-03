package com.exasol.adapter.dynamodb.mapping;

import java.util.Map;

/**
 * Exception that is thrown on lookup error. It is caught in
 * {@link AbstractColumnMappingDefinition#convertRow(Map)} and handled according
 * to {@link AbstractColumnMappingDefinition#getLookupFailBehaviour()}
 */
public class LookupColumnMappingException extends ColumnMappingException {

	/**
	 * Creates an instance of {@link LookupColumnMappingException}.
	 *
	 * @param message
	 *            Exception message
	 * @param column
	 *            {@link AbstractColumnMappingDefinition} that caused exception
	 */
    public LookupColumnMappingException(final String message, final AbstractColumnMappingDefinition column) {
		super(message, column);
	}
}
