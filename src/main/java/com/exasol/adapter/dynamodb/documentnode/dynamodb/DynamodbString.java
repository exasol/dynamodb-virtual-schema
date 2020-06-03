package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;

/**
 * This class represents a DynamoDB string value.
 */
public class DynamodbString implements DocumentValue<DynamodbNodeVisitor> {
    private static final long serialVersionUID = -8078166022358932302L;
    private final String value;

    /**
     * Create an instance of {@link DynamodbString}.
     * 
     * @param value value to hold
     */
    public DynamodbString(final String value) {
        this.value = value;
    }

    /**
     * Get the string value.
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

    @Override
    public String toString() {
        return "DynamodbString{" + "value='" + this.value + "'" + "}";
    }
}
