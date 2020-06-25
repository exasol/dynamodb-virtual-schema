package com.exasol.adapter.dynamodb.mapping;

/**
 * This exception is thrown on errors during the schema mapping. It is passed to the user.
 */
public class SchemaMappingException extends RuntimeException {
    private static final long serialVersionUID = 4440950553927795128L;

    /**
     * Create an instance of {@link SchemaMappingException}.
     * 
     * @param message exception message
     */
    public SchemaMappingException(final String message) {
        super(message);
    }
}
