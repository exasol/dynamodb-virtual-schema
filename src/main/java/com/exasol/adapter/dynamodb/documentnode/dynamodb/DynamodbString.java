package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;

/**
 * This class represents a DynamoDB string value.
 */
public class DynamodbString implements DocumentValue<DynamodbNodeVisitor> {
    private final String value;

    /**
     * Creates an instance of {@link DynamodbString}.
     * 
     * @param value value to hold
     */
    public DynamodbString(final String value) {
        this.value = value;
    }

    /**
     * Gives the string value.
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
