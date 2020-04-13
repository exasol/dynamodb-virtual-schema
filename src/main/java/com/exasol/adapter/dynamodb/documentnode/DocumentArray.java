package com.exasol.adapter.dynamodb.documentnode;

import java.util.List;

/**
 * Interface for array / list document nodes.
 */
public interface DocumentArray extends DocumentNode {

    /**
     * Gives a list with document nodes wrapping the values of the list wrapped in this node.
     * @return list of document nodes.
     */
    public List<DocumentNode> getValueList();

    /**
     * Gives a document node for an specific element of the wrapped array.
     * @param index index of the element that shall be returned
     * @return Document node wrapping the value.
     */
    public DocumentNode getValue(int index);

    @Override
    default void accept(final DocumentNodeVisitor visitor) {
        visitor.visit(this);
    }
}
