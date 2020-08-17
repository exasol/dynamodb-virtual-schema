package com.exasol.adapter.document.mapping.reader;

/**
 * Exception that is thrown on mapping failures.
 */
public class ExasolDocumentMappingLanguageException extends RuntimeException {
    private static final long serialVersionUID = -2085914528874432089L;

    /**
     * Create an instance of {@link ExasolDocumentMappingLanguageException}.
     *
     * @param message Exception message
     */
    public ExasolDocumentMappingLanguageException(final String message) {
        super(message);
    }
}
