package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.DocumentValue;

/**
 * This class wraps all other {@link AttributeValue} types that list and map.
 */
public class DynamodbValue implements DocumentValue {
    private final AttributeValue value;

    DynamodbValue(final AttributeValue value) {
        this.value = value;
    }

    public AttributeValue getValue() {
        return this.value;
    }
}
