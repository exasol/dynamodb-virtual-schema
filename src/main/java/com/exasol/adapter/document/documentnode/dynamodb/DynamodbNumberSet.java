package com.exasol.adapter.document.documentnode.dynamodb;

import java.math.BigDecimal;
import java.util.*;

import com.exasol.adapter.document.documentnode.DocumentArray;
import com.exasol.adapter.document.documentnode.DocumentDecimalValue;
import com.exasol.adapter.document.documentnode.holder.BigDecimalHolderNode;

/**
 * This class represents a DynamoDB number set value.
 */
public class DynamodbNumberSet implements DocumentArray {
    private final Collection<String> value;

    /**
     * Create an instance of {@link DynamodbNumberSet}.
     *
     * @param value value to hold
     */
    public DynamodbNumberSet(final Collection<String> value) {
        this.value = value;
    }

    @Override
    public List<DocumentDecimalValue> getValuesList() {
        final List<DocumentDecimalValue> list = new ArrayList<>();
        for (final String s : this.value) {
            final DocumentDecimalValue dynamodbNumber = new BigDecimalHolderNode(new BigDecimal(s));
            list.add(dynamodbNumber);
        }
        return list;
    }

    @Override
    public DocumentDecimalValue getValue(final int index) {
        return this.getValuesList().get(index);
    }

    @Override
    public int size() {
        return this.value.size();
    }

    Collection<String> getValue() {
        return this.value;
    }
}
