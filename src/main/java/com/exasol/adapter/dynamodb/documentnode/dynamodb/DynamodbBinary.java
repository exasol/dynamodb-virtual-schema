package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;

import software.amazon.awssdk.core.SdkBytes;

/**
 * This class represents a DynamoDB binary value.
 */
public class DynamodbBinary implements DocumentValue<DynamodbNodeVisitor> {
    private static final long serialVersionUID = -1085912088779479403L;
    private final byte[] value;

    /**
     * Create an instance of {@link DynamodbBinary}.
     *
     * @param value value to hold
     */
    public DynamodbBinary(final SdkBytes value) {
        this.value = value.asByteArray();
    }

    /**
     * Get the binary value.
     *
     * @return value of the string
     */
    public SdkBytes getValue() {
        return SdkBytes.fromByteArray(this.value);
    }

    @Override
    public void accept(final DynamodbNodeVisitor visitor) {
        visitor.visit(this);
    }
}
