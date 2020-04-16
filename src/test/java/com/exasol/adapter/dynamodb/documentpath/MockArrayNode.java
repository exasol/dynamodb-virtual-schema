package com.exasol.adapter.dynamodb.documentpath;

import java.util.List;

import com.exasol.adapter.dynamodb.documentnode.DocumentArray;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

public class MockArrayNode implements DocumentArray<MockVisitor> {
    private final List<DocumentNode<MockVisitor>> value;

    public MockArrayNode(final List<DocumentNode<MockVisitor>> value) {
        this.value = value;
    }

    @Override
    public List<DocumentNode<MockVisitor>> getValuesList() {
        return this.value;
    }

    @Override
    public DocumentNode<MockVisitor> getValue(final int index) {
        return this.value.get(index);
    }

    @Override
    public void accept(final MockVisitor visitor) {

    }
}
