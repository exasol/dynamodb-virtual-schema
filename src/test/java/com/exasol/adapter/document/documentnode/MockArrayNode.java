package com.exasol.adapter.document.documentnode;

import java.util.List;

public class MockArrayNode implements DocumentArray<Object> {
    private final List<DocumentNode<Object>> value;

    public MockArrayNode(final List<DocumentNode<Object>> value) {
        this.value = value;
    }

    @Override
    public List<DocumentNode<Object>> getValuesList() {
        return this.value;
    }

    @Override
    public DocumentNode<Object> getValue(final int index) {
        return this.value.get(index);
    }

    @Override
    public int size() {
        return this.value.size();
    }

    @Override
    public void accept(final Object visitor) {

    }
}
