package com.exasol.adapter.dynamodb.mapping;

/**
 * Exception on failures in column mapping
 */
public class ColumnValueExtractorException extends RuntimeException {
    private final ColumnMapping causingColumn;

    /**
     * Create an instance of {@link ColumnValueExtractorException}.
     *
     * @param message Exception message
     * @param column  {@link ColumnMapping} that caused exception
     */
    ColumnValueExtractorException(final String message, final ColumnMapping column) {
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
