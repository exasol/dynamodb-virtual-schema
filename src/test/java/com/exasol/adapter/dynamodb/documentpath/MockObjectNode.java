package com.exasol.adapter.dynamodb.documentpath;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.DocumentObject;

import java.util.Map;

class MockObjectNode implements DocumentObject {
    private final Map<String, DocumentNode> value;

    MockObjectNode(final Map<String, DocumentNode> value) {
        this.value = value;
    }

    @Override
    public Map<String, DocumentNode> getKeyValueMap() {
        return this.value;
    }

    @Override
    public DocumentNode get(final String key) {
        return this.value.get(key);
    }

    @Override
    public boolean hasKey(final String key) {
        return this.value.containsKey(key);
    }
}
