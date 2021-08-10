package com.exasol.adapter.document.documentnode.dynamodb;

import java.util.*;

import com.exasol.adapter.document.documentnode.DocumentArray;
import com.exasol.adapter.document.documentnode.DocumentStringValue;
import com.exasol.adapter.document.documentnode.holder.StringHolderNode;

/**
 * This class represents a DynamoDB string set value.
 */
public class DynamodbStringSet implements DocumentArray {
    private final Collection<String> value;

    /**
     * Create an instance of {@link DynamodbStringSet}.
     *
     * @param value value to hold
     */
    public DynamodbStringSet(final Collection<String> value) {
        this.value = value;
    }

    @Override
    public List<DocumentStringValue> getValuesList() {
        final List<DocumentStringValue> result = new ArrayList<>();
        for (final String string : this.value) {
            final StringHolderNode stringHolderNode = new StringHolderNode(string);
            result.add(stringHolderNode);
        }
        return result;
    }

    @Override
    public DocumentStringValue getValue(final int index) {
        return this.getValuesList().get(index);
    }

    @Override
    public int size() {
        return this.value.size();
    }
}
