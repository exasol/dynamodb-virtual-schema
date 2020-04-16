package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import java.nio.ByteBuffer;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;

/**
 * This class represents a DynamoDB binary value.
 */
public class DynamodbBinary implements DocumentValue<DynamodbNodeVisitor> {
    private final ByteBuffer value;

    /**
     * Creates an instance of {@link DynamodbBinary}.
     *
     * @param value value to hold
     */
    public DynamodbBinary(final ByteBuffer value) {
        this.value = value;
    }

    /**
     * Gives the binary value.
     *
     * @return value of the string
     */
    public ByteBuffer getValue() {
        return this.value;
    }

    @Override
    public void accept(final DynamodbNodeVisitor visitor) {
        visitor.visit(this);
    }
}
