package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.DocumentArray;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

/**
 * This class represents a DynamoDB list value.
 */
public class DynamodbList implements DocumentArray<DynamodbNodeVisitor> {
    private static final long serialVersionUID = 4513516964977921365L;
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
    public List<DocumentNode<DynamodbNodeVisitor>> getValuesList() {
        final DynamodbDocumentNodeFactory nodeFactory = new DynamodbDocumentNodeFactory();
        return this.value.stream().map(nodeFactory::buildDocumentNode).collect(Collectors.toList());
    }

    @Override
    public DocumentNode<DynamodbNodeVisitor> getValue(final int index) {
        return new DynamodbDocumentNodeFactory().buildDocumentNode(this.value.get(index));
    }

    @Override
    public int size() {
        return this.value.size();
    }

    @Override
    public void accept(final DynamodbNodeVisitor visitor) {
        visitor.visit(this);
    }

    List<AttributeValue> getValue() {
        return this.value;
    }
}
