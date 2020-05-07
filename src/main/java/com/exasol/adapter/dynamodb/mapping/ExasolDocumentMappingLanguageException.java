package com.exasol.adapter.dynamodb.mapping;

/**
 * Exception that is thrown on mapping failures.
 */
public class ExasolDocumentMappingLanguageException extends RuntimeException {
    private static final long serialVersionUID = -2085914528874432089L;

    /**
     * Creates an instance of {@link ExasolDocumentMappingLanguageException}.
     *
     * @param message Exception message
     */
    public ExasolDocumentMappingLanguageException(final String message) {
        super(message);
    }
}
