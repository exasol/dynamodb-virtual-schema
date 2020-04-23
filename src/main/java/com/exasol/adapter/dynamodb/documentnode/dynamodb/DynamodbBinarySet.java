package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.documentnode.DocumentArray;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

/**
 * This class represents a DynamoDB binary set value.
 */
public class DynamodbBinarySet implements DocumentArray<DynamodbNodeVisitor> {
    private static final long serialVersionUID = 6474569329954861497L;
    private final Collection<ByteBuffer> value;

    /**
     * Creates an instance of {@link DynamodbBinarySet}.
     *
     * @param value value to hold
     */
    public DynamodbBinarySet(final Collection<ByteBuffer> value) {
        this.value = value;
    }

    @Override
    public List<DocumentNode<DynamodbNodeVisitor>> getValuesList() {
        return this.value.stream().map(DynamodbBinary::new).collect(Collectors.toList());
    }

    @Override
    public DocumentNode<DynamodbNodeVisitor> getValue(final int index) {
        return this.getValuesList().get(index);
    }

    @Override
    public void accept(final DynamodbNodeVisitor visitor) {
        visitor.visit(this);
    }

    Collection<ByteBuffer> getValue() {
        return this.value;
    }
}
