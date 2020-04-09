package com.exasol.adapter.dynamodb.documentpath;

/**
 * This path segment describes that all children of an array are part of the path. E.g. topics.[*]
 */
public class ArrayAllPathSegment implements PathSegment {
    @Override
    public void accept(final PathSegmentVisitor visitor) {
        visitor.visit(this);
    }
}
