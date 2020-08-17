package com.exasol.adapter.document.documentpath;

/**
 * This path segment describes that all children of an array are part of the path.
 */
public class ArrayAllPathSegment implements PathSegment {
    private static final long serialVersionUID = 1742087357300081487L;

    @Override
    public void accept(final PathSegmentVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ArrayAllPathSegment;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
