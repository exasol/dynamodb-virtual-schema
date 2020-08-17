package com.exasol.adapter.document.mapping;

/**
 * Exception on failures in column mapping
 */
public class ColumnValueExtractorException extends SchemaMappingException {
    private static final long serialVersionUID = 5190776040871980095L;
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
     * Create an instance of {@link ColumnValueExtractorException}.
     *
     * @param message Exception message
     * @param cause   Exception that caused this exception
     * @param column  {@link ColumnMapping} that caused exception
     */
    ColumnValueExtractorException(final String message, final Exception cause, final ColumnMapping column) {
        super(message, cause);
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
