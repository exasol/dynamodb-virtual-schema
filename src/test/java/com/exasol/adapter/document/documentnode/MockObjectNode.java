package com.exasol.adapter.document.documentnode;

import java.util.Map;

public class MockObjectNode implements DocumentObject<Object> {
    private static final long serialVersionUID = -8862311988922376399L;
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
