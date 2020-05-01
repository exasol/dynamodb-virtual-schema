package com.exasol.adapter.dynamodb.queryrunner;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * This interface defines the access to the addValue method of {@link DynamodbValueListBuilder}.
 */
public interface DynamodbValueListBuilderAddValueInterface {
    /**
     * Adds a value to the expression list and returns an unique identifier for it.
     * 
     * @param valueToAdd value to be added to the list
     * @return unique identifier for the value
     */
    public String addValue(AttributeValue valueToAdd);
}
