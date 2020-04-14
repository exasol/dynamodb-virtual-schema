package com.exasol.adapter.dynamodb.documentpath;

import java.util.Map;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.DocumentObject;

public class MockObjectNode implements DocumentObject<MockVisitor> {
    private final Map<String, DocumentNode<MockVisitor>> value;

    public MockObjectNode(final Map<String, DocumentNode<MockVisitor>> value) {
        this.value = value;
    }

    @Override
    public Map<String, DocumentNode<MockVisitor>> getKeyValueMap() {
        return this.value;
    }

    @Override
    public DocumentNode<MockVisitor> get(final String key) {
        return this.value.get(key);
    }

    @Override
    public boolean hasKey(final String key) {
        return this.value.containsKey(key);
    }

    @Override
    public void accept(final MockVisitor visitor) {

    }
}
