package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.documentnode.DocumentArray;

/**
 * This class represents a DynamoDB string set value.
 */
public class DynamodbStringSet implements DocumentArray<DynamodbNodeVisitor> {
    private static final long serialVersionUID = 9102250461309769820L;
    private final Collection<String> value;

    /**
     * Creates an instance of {@link DynamodbStringSet}.
     *
     * @param value value to hold
     */
    public DynamodbStringSet(final Collection<String> value) {
        this.value = value;
    }

    @Override
    public List<DynamodbString> getValuesList() {
        return this.value.stream().map(DynamodbString::new).collect(Collectors.toList());
    }

    @Override
    public DynamodbString getValue(final int index) {
        return this.getValuesList().get(index);
    }

    @Override
    public void accept(final DynamodbNodeVisitor visitor) {
        visitor.visit(this);
    }

    Collection<String> getValue() {
        return this.value;
    }
}
