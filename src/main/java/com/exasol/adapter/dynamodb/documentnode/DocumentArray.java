package com.exasol.adapter.dynamodb.documentnode;

import java.util.List;

/**
 * Interface for array document nodes.
 */
public interface DocumentArray extends DocumentNode {
    List<DocumentNode> getValueList();

    @Override
    default void accept(final DocumentNodeVisitor visitor) {
        visitor.visit(this);
    }
}
