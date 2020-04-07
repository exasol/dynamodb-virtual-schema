package com.exasol.adapter.dynamodb.documentpath;

/**
 * Path segment defining an key lookup of an object e.g object.property
 */
public class ObjectPathSegment implements PathSegment {
    private final String lookupKey;

    /**
     * Creates an instance of {@link ObjectPathSegment}
     * 
     * @param lookupKey key to look up
     */
    public ObjectPathSegment(final String lookupKey) {
        this.lookupKey = lookupKey;
    }

    /**
     * Gives the lookup key of this segment.
     * 
     * @return lookup key
     */
    public String getLookupKey() {
        return this.lookupKey;
    }

    @Override
    public void accept(final PathSegmentVisitor visitor) {
        visitor.visit(this);
    }
}
