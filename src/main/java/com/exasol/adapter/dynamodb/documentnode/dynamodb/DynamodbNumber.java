package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;

/**
 * This class represents a DynamoDB number value.
 */
public class DynamodbNumber implements DocumentValue<DynamodbNodeVisitor> {
    private final String value;

    /**
     * Creates an instance of {@link DynamodbNumber}.
     *
     * @param value value to hold
     */
    public DynamodbNumber(final String value) {
        this.value = value;
    }

    /**
     * Gives the number value.
     *
     * @return value of the string
     */
    public String getValue() {
        return this.value;
    }

    @Override
    public void accept(final DynamodbNodeVisitor visitor) {
        visitor.visit(this);
    }
}
