package com.exasol.adapter.document.documentpath;

/**
 * This path segment defines an array lookup in an path expression.
 */
public class ArrayLookupPathSegment implements PathSegment {
    private static final long serialVersionUID = 2894278065231221419L;
    private final int lookupIndex;

    /**
     * Create an {@link ArrayLookupPathSegment}.
     * 
     * @param lookupIndex index to look up
     */
    public ArrayLookupPathSegment(final int lookupIndex) {
        this.lookupIndex = lookupIndex;
    }

    /**
     * Gets the array lookup index that this path segment describes.
     * 
     * @return array lookup index
     */
    public int getLookupIndex() {
        return this.lookupIndex;
    }

    @Override
    public void accept(final PathSegmentVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ArrayLookupPathSegment)) {
            return false;
        }
        final ArrayLookupPathSegment that = (ArrayLookupPathSegment) other;
        return this.lookupIndex == that.lookupIndex;
    }

    @Override
    public int hashCode() {
        return this.lookupIndex;
    }
}
