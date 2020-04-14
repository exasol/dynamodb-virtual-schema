package com.exasol.adapter.dynamodb.documentnode;

/**
 * This is a simple interface for accessing document data. It is used to abstract from the value representations of
 * different document databases. It accepts an generic visitor.
 */

@java.lang.SuppressWarnings("squid:S119") // VisitorType does not fit naming conventions.
public interface DocumentNode<VisitorType> {
    /**
     * Accept a VisitorType visitor.
     * 
     * @param visitor generic visitor to accept
     */
    public void accept(VisitorType visitor);
}
