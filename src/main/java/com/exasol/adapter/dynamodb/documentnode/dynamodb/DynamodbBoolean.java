package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;

/**
 * This class represents a DynamoDB boolean value.
 */
public class DynamodbBoolean implements DocumentValue<DynamodbNodeVisitor> {
    private final boolean value;

    /**
     * Creates an instance of {@link DynamodbBoolean}.
     *
     * @param value value to hold
     */
    public DynamodbBoolean(final boolean value) {
        this.value = value;
    }

    /**
     * Gives the boolean value.
     *
     * @return boolean value
     */
    public boolean getValue() {
        return this.value;
    }

    @Override
    public void accept(final DynamodbNodeVisitor visitor) {
        visitor.visit(this);
    }
}
