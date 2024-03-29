package com.exasol.adapter.document.documentnode.dynamodb;

import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.document.documentnode.DocumentArray;
import com.exasol.adapter.document.documentnode.DocumentNode;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * This class represents a DynamoDB list value.
 */
public class DynamodbList implements DocumentArray {
    private final List<AttributeValue> value;

    /**
     * Create an instance of {@link DynamodbList}.
     *
     * @param value value to hold
     */
    public DynamodbList(final List<AttributeValue> value) {
        this.value = value;
    }

    @Override
    public List<DocumentNode> getValuesList() {
        final DynamodbDocumentNodeFactory nodeFactory = new DynamodbDocumentNodeFactory();
        return this.value.stream().map(nodeFactory::buildDocumentNode).collect(Collectors.toList());
    }

    @Override
    public DocumentNode getValue(final int index) {
        return new DynamodbDocumentNodeFactory().buildDocumentNode(this.value.get(index));
    }

    @Override
    public int size() {
        return this.value.size();
    }
}
