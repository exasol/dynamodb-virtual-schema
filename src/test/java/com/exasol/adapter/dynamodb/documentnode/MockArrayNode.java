package com.exasol.adapter.dynamodb.documentnode;

import java.util.List;

public class MockArrayNode implements DocumentArray {
    private final List<DocumentNode> value;

    public MockArrayNode(final List<DocumentNode> value) {
        this.value = value;
    }

    @Override
    public List<DocumentNode> getValueList() {
        return this.value;
    }

    @Override
    public DocumentNode getValue(final int index) {
        return this.value.get(index);
    }
}
