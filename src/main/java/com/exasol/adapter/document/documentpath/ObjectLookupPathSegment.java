package com.exasol.adapter.document.documentpath;

/**
 * Path segment defining a key lookup of an object.
 */
public class ObjectLookupPathSegment implements PathSegment {
    private static final long serialVersionUID = -753364340469270540L;
    private final String lookupKey;

    /**
     * Create an instance of {@link ObjectLookupPathSegment}.
     * 
     * @param lookupKey key to look up
     */
    public ObjectLookupPathSegment(final String lookupKey) {
        this.lookupKey = lookupKey;
    }

    /**
     * Get the lookup key of this segment.
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

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ObjectLookupPathSegment)) {
            return false;
        }
        final ObjectLookupPathSegment that = (ObjectLookupPathSegment) other;
        return this.lookupKey.equals(that.lookupKey);
    }

    @Override
    public int hashCode() {
        return this.lookupKey.hashCode();
    }
}
