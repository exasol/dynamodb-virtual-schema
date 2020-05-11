package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

/**
 * Exception that is thrown on lookup error. It is caught in
 * {@link AbstractValueMapper#mapRow(DocumentNode, com.exasol.adapter.dynamodb.documentpath.PathIterationStateProvider)}
 * and handled according to {@link ColumnMappingDefinition#getLookupFailBehaviour()}
 */
public class LookupValueMapperException extends ValueMapperException {

    /**
     * Creates an instance of {@link LookupValueMapperException}.
     *
     * @param message Exception message
     * @param column  {@link ColumnMappingDefinition} that caused exception
     */
    LookupValueMapperException(final String message, final ColumnMappingDefinition column) {
        super(message, column);
    }
}
