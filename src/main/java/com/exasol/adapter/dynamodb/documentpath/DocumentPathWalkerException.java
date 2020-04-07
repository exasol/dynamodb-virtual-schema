package com.exasol.adapter.dynamodb.documentpath;

/**
 * Exception thrown when the modeled path does not exist.
 */
public class DocumentPathWalkerException extends Exception {
    private final String currentPath;

    DocumentPathWalkerException(final String message, final String currentPath) {
        super(message + " (current path= " + currentPath + ")");
        this.currentPath = currentPath;
    }

    /**
     * Gives the path at which the Exception occurred.
     * 
     * @return path description
     */
    public String getCurrentPath() {
        return this.currentPath;
    }
}