package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.DocumentObject;

/**
 * This class wraps DynamoDB objects. That means {@link AttributeValue}s containing a map.
 */
public class DynamodbObject implements DocumentObject {
    private final AttributeValue value;

    DynamodbObject(final AttributeValue value) {
        this.value = value;
    }

    @Override
    public Map<String, DocumentNode> getKeyValueMap() {
        final DynamodbDocumentNodeFactory nodeFactory = new DynamodbDocumentNodeFactory();
        return this.value.getM().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> nodeFactory.buildDocumentNode(entry.getValue())));
    }

    @Override
    public DocumentNode get(final String key) {
        return new DynamodbDocumentNodeFactory().buildDocumentNode(this.value.getM().get(key));
    }

    @Override
    public boolean hasKey(final String key) {
        return this.value.getM().containsKey(key);
    }
}
