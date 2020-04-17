package com.exasol.adapter.dynamodb.documentpath;

import java.io.Serializable;

/**
 * Interface for path segments used in a {@link DocumentPathExpression}.
 */
public interface PathSegment extends Serializable {
    /**
     * Accepts a {@link PathSegmentVisitor}
     * 
     * @param visitor to accept
     */
    void accept(PathSegmentVisitor visitor);
}
