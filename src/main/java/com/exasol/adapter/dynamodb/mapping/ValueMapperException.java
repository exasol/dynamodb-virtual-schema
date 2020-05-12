package com.exasol.adapter.dynamodb.mapping;

/**
 * Exception on failures in column mapping
 */
public class ValueMapperException extends RuntimeException {
    private final ColumnMapping causingColumn;

    /**
     * Creates an instance of {@link ValueMapperException}.
     *
     * @param message Exception message
     * @param column  {@link ColumnMapping} that caused exception
     */
    ValueMapperException(final String message, final ColumnMapping column) {
        super(message);
        this.causingColumn = column;
    }

    /**
     * Get the column that caused this exception.
     *
     * @return {@link ColumnMapping} that caused exception
     */
    public ColumnMapping getCausingColumn() {
        return this.causingColumn;
    }
}
