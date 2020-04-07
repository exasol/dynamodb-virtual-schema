package com.exasol.adapter.dynamodb.documentpath;

/**
 * Visitor interface for {@link PathSegment}
 */
public interface PathSegmentVisitor {

    /**
     * Visits a {@link ObjectPathSegment}
     * 
     * @param objectPathSegment to visit
     */
    public void visit(ObjectPathSegment objectPathSegment);
}
