package com.exasol.adapter.dynamodb.mapping;

/**
 * This enum describes behaviour of the mapping definition when the requested property is not set in a given DynamoDB
 * row.
 */
public enum LookupFailBehaviour {
    /**
     * Break the execution of the query .
     */
    EXCEPTION,
    /**
     * The column specific default value is returned.
     */
    DEFAULT_VALUE
}
