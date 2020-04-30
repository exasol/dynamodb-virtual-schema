package com.exasol.adapter.dynamodb.documentpath;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

/**
 * This factory creates a fitting {@link DocumentPathIterator} for a given path and document.
 */
@java.lang.SuppressWarnings("squid:S119") // VisitorType does not fit naming conventions.
public class DocumentPathIteratorFactory<VisitorType> {

    /**
     * Creates a fitting {@link DocumentPathIterator} for a given path and document.
     * 
     * @param path     path expression
     * @param document document
     * @return fitting {@link DocumentPathIterator}
     */
    public DocumentPathIterator buildFor(final DocumentPathExpression path, final DocumentNode<VisitorType> document) {
        if (path.indexOfFirstArrayAllSegment() == -1) {
            return new StaticDocumentPathIterator();
        } else {
            return new LoopDocumentPathIterator<>(path, document);
        }
    }
}
