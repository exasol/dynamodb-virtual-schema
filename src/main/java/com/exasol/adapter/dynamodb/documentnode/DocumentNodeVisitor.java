package com.exasol.adapter.dynamodb.documentnode;

/**
 * Visitor for {@link DocumentNode}
 */
public interface DocumentNodeVisitor {
    public void visit(DocumentArray array);

    public void visit(DocumentObject object);

    public void visit(DocumentValue value);
}
