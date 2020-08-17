package com.exasol.adapter.document.documentfetcher.dynamodb;

import com.exasol.adapter.document.documentnode.DocumentValue;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNodeVisitor;

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
