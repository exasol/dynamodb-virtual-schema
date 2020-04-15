package com.exasol.adapter.dynamodb.documentnode;

/**
 * This is a simple interface for accessing document data. It is used to abstract from the value representations of
 * different document databases. It accepts a generic visitor. See /doc/diagrams/documentnode.puml
 */

@java.lang.SuppressWarnings("squid:S119") // VisitorType does not fit naming conventions.
public interface DocumentNode<VisitorType> {
    /**
     * Accepts a VisitorType visitor.
     * 
     * @param visitor generic visitor to accept
     */
    public void accept(VisitorType visitor);
}
