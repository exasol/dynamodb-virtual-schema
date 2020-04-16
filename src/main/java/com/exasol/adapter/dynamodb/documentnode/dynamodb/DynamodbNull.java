package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;

/**
 * This class represents a DynamoDB null value.
 */
public class DynamodbNull implements DocumentValue<DynamodbNodeVisitor> {
    @Override
    public void accept(final DynamodbNodeVisitor visitor) {
        visitor.visit(this);
    }
}
