package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;

/**
 * This class builds a placeholder map for DynamoDB values.
 */
public class DynamodbAttributeValuePlaceholderMapBuilder
        extends AbstractDynamodbPlaceholderMapBuilder<DocumentValue<DynamodbNodeVisitor>> {

    @Override
    protected String getPlaceholderCharacter() {
        return ":";
    }
}
