package com.exasol.adapter.document.documentpath;

import java.util.Optional;

import com.exasol.adapter.document.documentnode.DocumentNode;

/**
 * This class is a simplified version of {@link DocumentPathWalker} for linear paths.
 */
@java.lang.SuppressWarnings("squid:S119") // VisitorType does not fit naming conventions.
public class LinearDocumentPathWalker<VisitorType> {

    private final DocumentPathWalker<VisitorType> documentPathWalker;

    /**
     * Create a {@link LinearDocumentPathWalker}. This walker has the limitation that it can only walk linear paths.
     * That is, the paths should not contain {@link ArrayAllPathSegment}s; otherwise it throws an exception.
     *
     * @param pathExpression Path definition. Must not contain {@link ArrayAllPathSegment}s.
     * @throws IllegalArgumentException if path contains {@link ArrayAllPathSegment}s.
     */
    public LinearDocumentPathWalker(final DocumentPathExpression pathExpression) {
        checkPathIsLinear(pathExpression);
        this.documentPathWalker = new DocumentPathWalker<>(pathExpression, new StaticDocumentPathIterator());
    }

    /**
     * Walks the path defined in constructor through the given document. In contrast to
     * {@link DocumentPathWalker#walkThroughDocument(DocumentNode)}, this method returns one single value.
     *
     * @param rootNode document to walk through
     * @return documents attribute described in {@link DocumentPathExpression} or an empty {@link Optional} if the path
     *         does not exist in the given document
     */
    public Optional<DocumentNode<VisitorType>> walkThroughDocument(final DocumentNode<VisitorType> rootNode) {
        return this.documentPathWalker.walkThroughDocument(rootNode);
    }

    private void checkPathIsLinear(final DocumentPathExpression pathExpression) {
        for (final PathSegment pathSegment : pathExpression.getSegments()) {
            if (pathSegment instanceof ArrayAllPathSegment) {
                throw new IllegalArgumentException(
                        "The given path is not a linear path. "
                                + "You can either remove the ArrayAllSegments from path or use a DocumentPathWalker.",
                        null);
            }
        }
    }
}
