package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * This class builds a placeholder map for DynamoDB values.
 */
public class DynamodbAttributeValuePlaceholderMapBuilder extends AbstractDynamodbPlaceholderMapBuilder<AttributeValue> {

    @Override
    protected String getPlaceholderCharacter() {
        return ":";
    }
}
