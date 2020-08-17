package com.exasol.adapter.document.documentnode.dynamodb;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.document.documentnode.DocumentArray;

import software.amazon.awssdk.core.SdkBytes;

/**
 * This class represents a DynamoDB binary set value.
 */
public class DynamodbBinarySet implements DocumentArray<DynamodbNodeVisitor> {
    private static final long serialVersionUID = 6474569329954861497L;
    private final List<DynamodbBinary> value;

    /**
     * Create an instance of {@link DynamodbBinarySet}.
     *
     * @param value value to hold
     */
    public DynamodbBinarySet(final Collection<SdkBytes> value) {
        this.value = value.stream().map(DynamodbBinary::new).collect(Collectors.toList());
    }

    @Override
    public List<DynamodbBinary> getValuesList() {
        return this.value;
    }

    @Override
    public DynamodbBinary getValue(final int index) {
        return this.getValuesList().get(index);
    }

    @Override
    public int size() {
        return this.value.size();
    }

    @Override
    public void accept(final DynamodbNodeVisitor visitor) {
        visitor.visit(this);
    }
}
