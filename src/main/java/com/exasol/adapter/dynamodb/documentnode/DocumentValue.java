package com.exasol.adapter.dynamodb.documentnode;

/**
 * Interface for simple values.
 */
public interface DocumentValue extends DocumentNode {
    @Override
    public default void accept(final DocumentNodeVisitor visitor) {
        visitor.visit(this);
    }
}
