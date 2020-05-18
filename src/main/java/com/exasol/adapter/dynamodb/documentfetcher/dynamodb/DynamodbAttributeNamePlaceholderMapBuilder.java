package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

/**
 * This class builds a placeholder map for DynamoDB attribute values.
 */
public class DynamodbAttributeNamePlaceholderMapBuilder extends AbstractDynamodbPlaceholderMapBuilder<String> {

    @Override
    protected String getPlaceholderCharacter() {
        return "#";
    }
}
