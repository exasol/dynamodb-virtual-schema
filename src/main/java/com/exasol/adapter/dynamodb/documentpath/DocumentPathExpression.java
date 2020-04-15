package com.exasol.adapter.dynamodb.documentpath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class allows to express path through a document.
 */
public class DocumentPathExpression {
    private final List<PathSegment> path;

    private DocumentPathExpression(final List<PathSegment> path) {
        this.path = path;
    }

    List<PathSegment> getPath() {
        return this.path;
    }

    /**
     * Creates a subpath from startIndex (inclusive) til endIndex (exclusive).
     * 
     * @param startIndex index in path for new path to start
     * @param endIndex   index in path for new path to end
     * @return {@link DocumentPathExpression} instance
     */
    public DocumentPathExpression getSubPath(final int startIndex, final int endIndex) {
        return new DocumentPathExpression(Collections.unmodifiableList(this.path.subList(startIndex, endIndex)));
    }

    /**
     * Gives the size of this path expression.
     * 
     * @return size
     */
    public int size() {
        return this.path.size();
    }

    /**
     * Builder for {@link DocumentPathExpression}.
     */
    public static class Builder {
        private final List<PathSegment> path = new ArrayList<>();

        /**
         * Appends a {@link PathSegment} to the current path.
         * 
         * @param segment path segment to append
         * @return this for as fluent programming interface
         */
        public Builder add(final PathSegment segment) {
            this.path.add(segment);
            return this;
        }

        /**
         * Appends an {@link ObjectLookupPathSegment} to the current path.
         * 
         * @param lookupKey lookup key for the {@link ObjectLookupPathSegment}
         * @return this for as fluent programming interface
         */
        public Builder addObjectLookup(final String lookupKey) {
            return this.add(new ObjectLookupPathSegment(lookupKey));
        }

        /**
         * Appends an {@link ArrayLookupPathSegment} to the current path.
         * 
         * @param lookupIndex lookup index for {@link ArrayLookupPathSegment}
         * @return this for as fluent programming interface
         */
        public Builder addArrayLookup(final int lookupIndex) {
            return this.add(new ArrayLookupPathSegment(lookupIndex));
        }

        /**
         * Appends an {@link ArrayAllPathSegment} to the current path.
         *
         * @return this for as fluent programming interface
         */
        public Builder addArrayAll() {
            return this.add(new ArrayAllPathSegment());
        }

        /**
         * Finishes the build process of {@link DocumentPathExpression}.
         * 
         * @return a new instance of {@link DocumentPathExpression}
         */
        public DocumentPathExpression build() {
            return new DocumentPathExpression(Collections.unmodifiableList(this.path));
        }
    }
}
