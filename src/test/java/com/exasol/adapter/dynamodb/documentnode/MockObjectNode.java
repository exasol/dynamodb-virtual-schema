package com.exasol.adapter.dynamodb.documentnode;

import java.util.Map;

//TODO replace by JSON
public class MockObjectNode implements DocumentObject<Object> {
    private final Map<String, DocumentNode<Object>> value;

    public MockObjectNode(final Map<String, DocumentNode<Object>> value) {
        this.value = value;
    }

    @Override
    public Map<String, DocumentNode<Object>> getKeyValueMap() {
        return this.value;
    }

    @Override
    public DocumentNode<Object> get(final String key) {
        return this.value.get(key);
    }

    @Override
    public boolean hasKey(final String key) {
        return this.value.containsKey(key);
    }

    @Override
    public void accept(final Object visitor) {

    }
}
