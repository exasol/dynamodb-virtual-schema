package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.DocumentValue;

public class DynamodbValue implements DocumentValue {
    private final AttributeValue value;

    DynamodbValue(final AttributeValue value) {
        this.value = value;
    }
}
