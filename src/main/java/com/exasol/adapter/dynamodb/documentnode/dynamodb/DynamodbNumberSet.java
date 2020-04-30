package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.documentnode.DocumentArray;

/**
 * This class represents a DynamoDB number set value.
 */
public class DynamodbNumberSet implements DocumentArray<DynamodbNodeVisitor> {
    private static final long serialVersionUID = 7289926315213567897L;
    private final Collection<String> value;

    /**
     * Creates an instance of {@link DynamodbNumberSet}.
     *
     * @param value value to hold
     */
    public DynamodbNumberSet(final Collection<String> value) {
        this.value = value;
    }

    @Override
    public List<DynamodbNumber> getValuesList() {
        return this.value.stream().map(DynamodbNumber::new).collect(Collectors.toList());
    }

    @Override
    public DynamodbNumber getValue(final int index) {
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

    Collection<String> getValue() {
        return this.value;
    }
}
