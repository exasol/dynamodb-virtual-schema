package com.exasol.adapter.document.mapping;

/**
 * Exception thrown if the size of a value is larger than the configured destination column size.
 */
public class OverflowException extends ColumnValueExtractorException {
    private static final long serialVersionUID = 3544045333431038655L;//

    /**
     * Create an instance of {@link OverflowException}.
     * 
     * @param message message
     * @param column  column that caused the exception
     */
    public OverflowException(final String message, final PropertyToColumnMapping column) {
        super(message, column);
    }
}
