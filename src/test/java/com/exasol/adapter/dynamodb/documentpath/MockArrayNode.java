package com.exasol.adapter.dynamodb.documentpath;

import com.exasol.adapter.dynamodb.documentnode.DocumentArray;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

import java.util.List;

class MockArrayNode implements DocumentArray {
    private final List<DocumentNode> value;

    MockArrayNode(final List<DocumentNode> value) {
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
