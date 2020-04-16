package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.DocumentObject;

/**
 * This class represents a DynamoDB map value.
 */
public class DynamodbObject implements DocumentObject<DynamodbNodeVisitor> {
    private final Map<String, AttributeValue> value;

    /**
     * Creates an instance of {@link DynamodbObject}.
     *
     * @param value value to hold
     */
    public DynamodbObject(final Map<String, AttributeValue> value) {
        this.value = value;
    }

    @Override
    public Map<String, DocumentNode<DynamodbNodeVisitor>> getKeyValueMap() {
        final DynamodbDocumentNodeFactory nodeFactory = new DynamodbDocumentNodeFactory();
        return this.value.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> nodeFactory.buildDocumentNode(entry.getValue())));
    }

    @Override
    public DocumentNode<DynamodbNodeVisitor> get(final String key) {
        return new DynamodbDocumentNodeFactory().buildDocumentNode(this.value.get(key));
    }

    @Override
    public boolean hasKey(final String key) {
        return this.value.containsKey(key);
    }

    @Override
    public void accept(final DynamodbNodeVisitor visitor) {
        visitor.visit(this);
    }
}
