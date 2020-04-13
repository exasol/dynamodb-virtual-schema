package com.exasol.adapter.dynamodb.documentpath;

/**
 * This path segment defines a array lookup in an path expression. E.g topics[0]
 */
public class ArrayLookupPathSegment implements PathSegment {
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
}
