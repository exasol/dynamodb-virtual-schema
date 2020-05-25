package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

/**
 * Exception that is thrown on lookup error. It is caught in
 * {@link ColumnValueExtractor#extractColumnValue(DocumentNode, com.exasol.adapter.dynamodb.documentpath.PathIterationStateProvider)}
 * and handled according to {@link PropertyToColumnMapping#getLookupFailBehaviour()}
 */
public class ColumnValueExtractorLookupException extends ColumnValueExtractorException {
    private static final long serialVersionUID = 6126777174742675113L;

    /**
     * Create an instance of {@link ColumnValueExtractorLookupException}.
     *
     * @param message Exception message
     * @param column  {@link ColumnMapping} that caused exception
     */
    ColumnValueExtractorLookupException(final String message, final ColumnMapping column) {
        super(message, column);
    }
}
