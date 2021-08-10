package com.exasol.adapter.document.documentfetcher.dynamodb;

import com.exasol.adapter.sql.SqlNode;

/**
 * This class builds a placeholder map for DynamoDB values.
 */
public class DynamodbAttributeValuePlaceholderMapBuilder extends AbstractDynamodbPlaceholderMapBuilder<SqlNode> {

    @Override
    protected String getPlaceholderCharacter() {
        return ":";
    }
}
