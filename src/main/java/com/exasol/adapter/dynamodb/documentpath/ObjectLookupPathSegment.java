package com.exasol.adapter.dynamodb.documentpath;

/**
 * Path segment defining a key lookup of an object.
 */
public class ObjectLookupPathSegment implements PathSegment {
    private static final long serialVersionUID = -753364340469270540L;
    private final String lookupKey;

    /**
     * Creates an instance of {@link ObjectLookupPathSegment}.
     * 
     * @param lookupKey key to look up
     */
    public ObjectLookupPathSegment(final String lookupKey) {
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
