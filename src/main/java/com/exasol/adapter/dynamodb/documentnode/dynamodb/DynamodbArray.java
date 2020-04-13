package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.DocumentArray;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

public class DynamodbArray implements DocumentArray {
    private final AttributeValue value;

    DynamodbArray(final AttributeValue value) {
        this.value = value;
    }

    @Override
    public List<DocumentNode> getValueList() {
        final DynamodbDocumentNodeFactory nodeFactory = new DynamodbDocumentNodeFactory();
        return this.value.getL().stream().map(nodeFactory::buildDocumentNode).collect(Collectors.toList());
    }

    @Override
    public DocumentNode getValue(final int index) {
        return new DynamodbDocumentNodeFactory().buildDocumentNode(this.value.getL().get(index));
    }
}
