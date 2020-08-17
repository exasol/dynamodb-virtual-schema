package com.exasol.adapter.document.mapping;

/**
 * This exception is thrown on errors during the schema mapping. It is passed to the user.
 */
public class SchemaMappingException extends RuntimeException {
    private static final long serialVersionUID = -8528943426430594404L;

    /**
     * Create an instance of {@link SchemaMappingException}.
     * 
     * @param message exception message
     */
    public SchemaMappingException(final String message) {
        super(message);
    }

    /**
     * Create an instance of {@link SchemaMappingException}.
     *
     * @param message Exception message
     * @param cause   Exception that caused this exception
     */
    public SchemaMappingException(final String message, final Exception cause) {
        super(message, cause);
    }
}
