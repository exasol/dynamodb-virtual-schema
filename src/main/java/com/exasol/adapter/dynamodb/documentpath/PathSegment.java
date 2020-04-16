package com.exasol.adapter.dynamodb.documentpath;

/**
 * Interface for path segments used in a {@link DocumentPathExpression}.
 */
public interface PathSegment {
    /**
     * Accepts a {@link PathSegmentVisitor}
     * 
     * @param visitor to accept
     */
    void accept(PathSegmentVisitor visitor);
}
