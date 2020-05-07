package com.exasol.adapter.dynamodb.mapping;

/**
 * Exception on failures in column mapping
 */
public class ValueMapperException extends RuntimeException {
    private final ColumnMappingDefinition causingColumn;

    /**
     * Creates an instance of {@link ValueMapperException}.
     *
     * @param message Exception message
     * @param column  {@link ColumnMappingDefinition} that caused exception
     */
    ValueMapperException(final String message, final ColumnMappingDefinition column) {
        super(message);
        this.causingColumn = column;
    }

    /**
     * Get the column that caused this exception.
     *
     * @return {@link ColumnMappingDefinition} that caused exception
     */
    public ColumnMappingDefinition getCausingColumn() {
        return this.causingColumn;
    }
}
