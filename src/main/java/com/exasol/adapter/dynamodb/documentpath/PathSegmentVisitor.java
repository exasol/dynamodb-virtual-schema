package com.exasol.adapter.dynamodb.documentpath;

/**
 * Visitor interface for {@link PathSegment}.
 */
public interface PathSegmentVisitor {

    /**
     * Visits a {@link ObjectLookupPathSegment}.
     * 
     * @param objectLookupPathSegment to visit
     */
    public void visit(ObjectLookupPathSegment objectLookupPathSegment);

    /**
     * Visits a {@link ArrayLookupPathSegment}.
     *
     * @param arrayLookupPathSegment to visit
     */
    public void visit(ArrayLookupPathSegment arrayLookupPathSegment);

    /**
     * Visits a {@link ArrayAllPathSegment}.
     *
     * @param arrayAllPathSegment to visit
     */
    public void visit(ArrayAllPathSegment arrayAllPathSegment);
}
