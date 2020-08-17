package com.exasol.adapter.document.documentnode.dynamodb;

import com.exasol.adapter.document.documentnode.DocumentValue;

/**
 * This class represents a DynamoDB null value.
 */
public class DynamodbNull implements DocumentValue<DynamodbNodeVisitor> {
    private static final long serialVersionUID = 3151845330477040681L;

    @Override
    public void accept(final DynamodbNodeVisitor visitor) {
        visitor.visit(this);
    }
}
