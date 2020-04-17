package com.exasol.adapter.dynamodb.documentpath;

/**
 * This path segment describes that all children of an array are part of the path.
 */
public class ArrayAllPathSegment implements PathSegment {
    private static final long serialVersionUID = 1742087357300081487L;

    @Override
    public void accept(final PathSegmentVisitor visitor) {
        visitor.visit(this);
    }
}
