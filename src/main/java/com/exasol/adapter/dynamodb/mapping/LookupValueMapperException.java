package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

/**
 * Exception that is thrown on lookup error. It is caught in
 * {@link ValueExtractor#mapRow(DocumentNode, com.exasol.adapter.dynamodb.documentpath.PathIterationStateProvider)} and
 * handled according to {@link PropertyToColumnMapping#getLookupFailBehaviour()}
 */
public class LookupValueMapperException extends ValueMapperException {
    private static final long serialVersionUID = 6126777174742675113L;

    /**
     * Creates an instance of {@link LookupValueMapperException}.
     *
     * @param message Exception message
     * @param column  {@link ColumnMapping} that caused exception
     */
    LookupValueMapperException(final String message, final ColumnMapping column) {
        super(message, column);
    }
}
