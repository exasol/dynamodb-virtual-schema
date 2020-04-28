package com.exasol.adapter.dynamodb.mapping;

/**
 * Exception on failures in column mapping
 */
public class ValueMapperException extends RuntimeException {
    private final AbstractColumnMappingDefinition causingColumn;

    /**
     * Creates an instance of {@link ValueMapperException}.
     *
     * @param message Exception message
     * @param column  {@link AbstractColumnMappingDefinition} that caused exception
     */
    ValueMapperException(final String message, final AbstractColumnMappingDefinition column) {
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
