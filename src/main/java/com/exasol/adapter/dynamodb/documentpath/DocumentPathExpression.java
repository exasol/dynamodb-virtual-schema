package com.exasol.adapter.dynamodb.documentpath;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class allows to express path through a document.
 */
public class DocumentPathExpression implements Serializable {
    private static final DocumentPathExpression EMPTY_PATH = new DocumentPathExpression(Collections.emptyList());
    private static final long serialVersionUID = -5010657725802907603L;
    private final ArrayList<PathSegment> segments;

    private DocumentPathExpression(final List<PathSegment> segments) {
        this.segments = new ArrayList<>(segments.size());
        this.segments.addAll(segments);
    }

    /**
     * Get an empty {@link DocumentPathExpression}.
     *
     * @return empty {@link DocumentPathExpression}
     */
    public static DocumentPathExpression empty() {
        return EMPTY_PATH;
    }

    /**
     * Get the list with the path segments.
     * 
     * @return list with path segments
     */
    public List<PathSegment> getSegments() {
        return this.segments;
    }

    /**
     * Create a subpath from startIndex (inclusive) til endIndex (exclusive).
     *
     * @param startIndex index in path for new path to start
     * @param endIndex   index in path for new path to end
     * @return {@link DocumentPathExpression} instance
     */
    public DocumentPathExpression getSubPath(final int startIndex, final int endIndex) {
        return new DocumentPathExpression(Collections.unmodifiableList(this.segments.subList(startIndex, endIndex)));
    }

    /**
     * Get the size of this path expression.
     *
     * @return size
     */
    public int size() {
        return this.segments.size();
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DocumentPathExpression)) {
            return false;
        }
        final DocumentPathExpression that = (DocumentPathExpression) other;
        return this.segments.equals(that.segments);
    }

    @Override
    public int hashCode() {
        return this.segments.hashCode();
    }

    @Override
    public String toString() {
        return new DocumentPathToStringConverter().convertToString(this);
    }

    /**
     * Get the index of the first {@link ArrayAllPathSegment}.
     *
     * @return index of the first {@link ArrayAllPathSegment}; {@code -1} if not found.
     */
    public int indexOfFirstArrayAllSegment() {
        int index = 0;
        for (final PathSegment pathSegment : this.segments) {
            if (pathSegment instanceof ArrayAllPathSegment) {
                return index;
            }
            index++;
        }
        return -1;
    }

    /**
     * Checks if this path expression starts with an other path expression.
     * 
     * @param other sub path expression
     * @return {@code true} if this path starts with the other path
     */
    public boolean startsWith(final DocumentPathExpression other) {
        if (other.size() > this.size()) {
            return false;
        }
        final DocumentPathExpression thisSubpathWithLengthOfOther = this.getSubPath(0, other.size());
        return thisSubpathWithLengthOfOther.equals(other);
    }

    /**
     * Builder for {@link DocumentPathExpression}.
     */
    public static class Builder {
        private final List<PathSegment> segments = new ArrayList<>();

        /**
         * Create an instance of {@link Builder} with an empty path.
         */
        public Builder() {
            // intentionally left empty.
        }

        /**
         * Create a copy of the given {@link Builder}.
         * 
         * @param copy {@link Builder} to copy
         */
        public Builder(final Builder copy) {
            this.segments.addAll(copy.segments);
        }

        /**
         * Append a {@link PathSegment} to the current path.
         * 
         * @param segment path segment to append
         * @return {@code this} instance for fluent programming
         */
        public Builder addPathSegment(final PathSegment segment) {
            this.segments.add(segment);
            return this;
        }

        /**
         * Append an {@link ObjectLookupPathSegment} to the current path.
         * 
         * @param lookupKey lookup key for the {@link ObjectLookupPathSegment}
         * @return {@code this} instance for fluent programming
         */
        public Builder addObjectLookup(final String lookupKey) {
            return this.addPathSegment(new ObjectLookupPathSegment(lookupKey));
        }

        /**
         * Append an {@link ArrayLookupPathSegment} to the current path.
         * 
         * @param lookupIndex lookup index for {@link ArrayLookupPathSegment}
         * @return {@code this} instance for fluent programming
         */
        public Builder addArrayLookup(final int lookupIndex) {
            return this.addPathSegment(new ArrayLookupPathSegment(lookupIndex));
        }

        /**
         * Append an {@link ArrayAllPathSegment} to the current path.
         *
         * @return {@code this} instance for fluent programming
         */
        public Builder addArrayAll() {
            return this.addPathSegment(new ArrayAllPathSegment());
        }

        /**
         * Finish the build process of {@link DocumentPathExpression}.
         * 
         * @return a new instance of {@link DocumentPathExpression}
         */
        public DocumentPathExpression build() {
            return new DocumentPathExpression(Collections.unmodifiableList(this.segments));
        }
    }
}
