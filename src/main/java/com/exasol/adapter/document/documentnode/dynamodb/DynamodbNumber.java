package com.exasol.adapter.document.documentnode.dynamodb;

import com.exasol.adapter.document.documentnode.DocumentValue;

/**
 * This class represents a DynamoDB number value.
 */
public class DynamodbNumber implements DocumentValue<DynamodbNodeVisitor> {
    private static final long serialVersionUID = 2920294613453224590L;
    private final String value;

    /**
     * Create an instance of {@link DynamodbNumber}.
     *
     * @param value value to hold
     */
    public DynamodbNumber(final String value) {
        this.value = value;
    }

    /**
     * Get the number value.
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
