package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

/**
 * This exception is thrown by {@link DynamodbQuerySelectionFilter} if the whitelisted columns can't be extracted
 * without shrinking the result space of the query.
 */
public class DynamodbQuerySelectionFilterException extends RuntimeException {
    private static final long serialVersionUID = -5314222741575889839L;

    /**
     * Creates an instance of {@link DynamodbQuerySelectionFilterException}
     * 
     * @param message exception message
     */
    public DynamodbQuerySelectionFilterException(final String message) {
        super(message);
    }
}
