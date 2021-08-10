package com.exasol.adapter.document.documentnode.dynamodb;

import java.util.*;

import com.exasol.adapter.document.documentnode.DocumentArray;
import com.exasol.adapter.document.documentnode.DocumentBinaryValue;
import com.exasol.adapter.document.documentnode.holder.BinaryHolderNode;

import software.amazon.awssdk.core.SdkBytes;

/**
 * This class represents a DynamoDB binary set value.
 */
public class DynamodbBinarySet implements DocumentArray {
    private final List<DocumentBinaryValue> value;

    /**
     * Create an instance of {@link DynamodbBinarySet}.
     *
     * @param value value to hold
     */
    public DynamodbBinarySet(final Collection<SdkBytes> value) {
        final List<DocumentBinaryValue> list = new ArrayList<>();
        for (final SdkBytes sdkBytes : value) {
            final byte[] asByteArray = sdkBytes.asByteArray();
            final BinaryHolderNode binaryHolderNode = new BinaryHolderNode(asByteArray);
            list.add(binaryHolderNode);
        }
        this.value = list;
    }

    @Override
    public List<DocumentBinaryValue> getValuesList() {
        return this.value;
    }

    @Override
    public DocumentBinaryValue getValue(final int index) {
        return this.getValuesList().get(index);
    }

    @Override
    public int size() {
        return this.value.size();
    }
}
