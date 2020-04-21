package com.exasol.adapter.dynamodb.documentpath;

/**
 * This path segment defines an array lookup in an path expression.
 */
public class ArrayLookupPathSegment implements PathSegment {
    private static final long serialVersionUID = 2894278065231221419L;
    private final int lookupIndex;

    /**
     * Creates an {@link ArrayLookupPathSegment}.
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
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final ArrayLookupPathSegment that = (ArrayLookupPathSegment) o;

        return this.lookupIndex == that.lookupIndex;
    }

    @Override
    public int hashCode() {
        return this.lookupIndex;
    }
}
