package com.exasol.adapter.dynamodb.documentnode;

/**
 * This is a simple interface for accessing document data. It is used to abstract from the value representations of
 * different document databases.
 */
public interface DocumentNode {
    /**
     * Accept {@link DocumentNodeVisitor}
     * 
     * @param visitor
     */
    public void accept(DocumentNodeVisitor visitor);
}
