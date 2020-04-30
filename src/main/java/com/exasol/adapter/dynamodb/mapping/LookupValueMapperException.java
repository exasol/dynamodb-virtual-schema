package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

/**
 * Exception that is thrown on lookup error. It is caught in {@link AbstractValueMapper#mapRow(DocumentNode)} and
 * handled according to {@link AbstractColumnMappingDefinition#getLookupFailBehaviour()}
 */
public class LookupValueMapperException extends ValueMapperException {

    /**
     * Creates an instance of {@link LookupValueMapperException}.
     *
     * @param message Exception message
     * @param column  {@link AbstractColumnMappingDefinition} that caused exception
     */
    LookupValueMapperException(final String message, final AbstractColumnMappingDefinition column) {
        super(message, column);
    }
}
