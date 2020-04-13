package com.exasol.adapter.dynamodb.documentnode;

/**
 * Visitor for {@link DocumentNode}.
 */
public interface DocumentNodeVisitor {

    /**
     * Visit an {@link DocumentArray}.
     * 
     * @param array {@link DocumentArray} to visit
     */
    public void visit(DocumentArray array);

    /**
     * Visit an {@link DocumentObject}.
     *
     * @param object {@link DocumentObject} to visit
     */
    public void visit(DocumentObject object);

    /**
     * Visit an {@link DocumentValue}.
     *
     * @param value {@link DocumentValue} to visit
     */
    public void visit(DocumentValue value);
}
