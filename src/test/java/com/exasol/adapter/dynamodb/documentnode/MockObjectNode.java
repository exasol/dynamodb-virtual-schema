package com.exasol.adapter.dynamodb.documentnode;

import java.util.Map;

public class MockObjectNode implements DocumentObject {
    private final Map<String, DocumentNode> value;

    public MockObjectNode(final Map<String, DocumentNode> value) {
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
