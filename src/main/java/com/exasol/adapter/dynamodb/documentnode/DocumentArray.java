package com.exasol.adapter.dynamodb.documentnode;

import java.util.List;

/**
 * Interface for array / list document nodes.
 */
@java.lang.SuppressWarnings("squid:S119") // VisitorType does not fit naming conventions.
public interface DocumentArray<VisitorType> extends DocumentNode<VisitorType> {

    /**
     * Returns a list with document nodes wrapping the values of the list wrapped in this node.
     * 
     * @return list of document nodes.
     */
    public List<? extends DocumentNode<VisitorType>> getValuesList();

    /**
     * Gives a document node for an specific element of the wrapped array.
     * 
     * @param index index of the element that shall be returned
     * @return Document node wrapping the value.
     */
    public DocumentNode<VisitorType> getValue(int index);
}
