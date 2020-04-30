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
    private final ArrayList<PathSegment> path;

    private DocumentPathExpression(final List<PathSegment> path) {
        this.path = new ArrayList<>(path.size());
        this.path.addAll(path);
    }

    /**
     * Gives an empty {@link DocumentPathExpression}.
     *
     * @return empty {@link DocumentPathExpression}
     */
    public static DocumentPathExpression empty() {
        return EMPTY_PATH;
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

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final DocumentPathExpression that = (DocumentPathExpression) other;

        return this.path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return this.path.hashCode();
    }

    @Override
    public String toString() {
        return new DocumentPathToStringConverter().convertToString(this);
    }

    /**
     * Gives the index of the first {@link ArrayAllPathSegment}.
     *
     * @return index of the first {@link ArrayAllPathSegment}; {@code -1} if not found.
     */
    int indexOfFirstArrayAllSegment() {
        int index = 0;
        for (final PathSegment pathSegment : this.path) {
            if (pathSegment instanceof ArrayAllPathSegment) {
                return index;
            }
            index++;
        }
        return -1;
    }

    /**
     * Checks if this path expression starts with an other path expression. If the path expressions are equal
     * {@code true} is returned.
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
        private final List<PathSegment> path = new ArrayList<>();

        /**
         * Creates an instance of {@link Builder} with an empty path.
         */
        public Builder() {
            // intentionally left empty.
        }

        /**
         * Creates a copy of the given {@link Builder}.
         * 
         * @param copy {@link Builder} to copy
         */
        public Builder(final Builder copy) {
            this.path.addAll(copy.path);
        }

        /**
         * Appends a {@link PathSegment} to the current path.
         * 
         * @param segment path segment to append
         * @return {@code this} instance for fluent programming
         */
        public Builder addPathSegment(final PathSegment segment) {
            this.path.add(segment);
            return this;
        }

        /**
         * Appends an {@link ObjectLookupPathSegment} to the current path.
         * 
         * @param lookupKey lookup key for the {@link ObjectLookupPathSegment}
         * @return {@code this} instance for fluent programming
         */
        public Builder addObjectLookup(final String lookupKey) {
            return this.addPathSegment(new ObjectLookupPathSegment(lookupKey));
        }

        /**
         * Appends an {@link ArrayLookupPathSegment} to the current path.
         * 
         * @param lookupIndex lookup index for {@link ArrayLookupPathSegment}
         * @return {@code this} instance for fluent programming
         */
        public Builder addArrayLookup(final int lookupIndex) {
            return this.addPathSegment(new ArrayLookupPathSegment(lookupIndex));
        }

        /**
         * Appends an {@link ArrayAllPathSegment} to the current path.
         *
         * @return {@code this} instance for fluent programming
         */
        public Builder addArrayAll() {
            return this.addPathSegment(new ArrayAllPathSegment());
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
