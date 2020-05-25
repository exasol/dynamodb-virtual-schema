package com.exasol.adapter.dynamodb.documentpath;

/**
 * This exception is thrown when the modeled path does not exist.
 */
public class DocumentPathWalkerException extends RuntimeException {
    private final String currentPath;

    DocumentPathWalkerException(final String message, final String currentPath) {
        super(message + (currentPath != null ? " (current path= " + currentPath + ")" : ""));
        this.currentPath = currentPath;
    }

    /**
     * Get the path at which the Exception occurred.
     * 
     * @return path description
     */
    public String getCurrentPath() {
        return this.currentPath;
    }
}
