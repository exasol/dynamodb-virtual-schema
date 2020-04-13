package com.exasol.adapter.dynamodb.documentpath;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

/**
 * This class is a simplified version of {@link DocumentPathWalker}. It has the limitation that it can only walk linear
 * paths. That means that the paths must not contain {@link ArrayAllPathSegment}s.
 *
 * In contrast to {@link DocumentPathWalker#walk(DocumentNode)} This classes walk method only returns one single value.
 */
public class LinearDocumentPathWalker {

    private final DocumentPathWalker documentPathWalker;

    /**
     * Creates a {@link LinearDocumentPathWalker}.
     * 
     * @param pathExpression Path definition. Must not contain {@link ArrayAllPathSegment}s.
     * @throws DocumentPathWalkerException if path contains {@link ArrayAllPathSegment}s.
     */
    public LinearDocumentPathWalker(final DocumentPathExpression pathExpression) throws DocumentPathWalkerException {
        checkPathIsLinear(pathExpression);
        this.documentPathWalker = new DocumentPathWalker(pathExpression);
    }

    /**
     * Walks the path defined in constructor through the given document.
     *
     * @param rootNode document to walk through
     * @return documents attribute described in {@link DocumentPathExpression}
     * @throws DocumentPathWalkerException if defined path does not exist in the given document
     */
    public DocumentNode walk(final DocumentNode rootNode) throws DocumentPathWalkerException {
        return this.documentPathWalker.walk(rootNode).get(0);
    }

    private void checkPathIsLinear(final DocumentPathExpression pathExpression) throws DocumentPathWalkerException {
        for (final PathSegment pathSegment : pathExpression.getPath()) {
            if (pathSegment instanceof ArrayAllPathSegment) {
                throw new DocumentPathWalkerException(
                        "The given path is not a linear path. "
                                + "You can either remove the ArrayAllSegments from path or use a DocumentPathWalker.",
                        null);
            }
        }
    }
}
