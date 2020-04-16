package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.documentnode.DocumentArray;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

/**
 * This class represents a DynamoDB string set value.
 */
public class DynamodbStringSet implements DocumentArray<DynamodbNodeVisitor> {
    private final Collection<String> value;

    /**
     * Creates an instance of {@link DynamodbStringSet}.
     *
     * @param value value to hold
     */
    public DynamodbStringSet(final Collection<String> value) {
        this.value = value;
    }

    @Override
    public List<DocumentNode<DynamodbNodeVisitor>> getValuesList() {
        return this.value.stream().map(DynamodbString::new).collect(Collectors.toList());
    }

    @Override
    public DocumentNode<DynamodbNodeVisitor> getValue(final int index) {
        return this.getValuesList().get(index);
    }

    @Override
    public void accept(final DynamodbNodeVisitor visitor) {
        visitor.visit(this);
    }
}
