package com.exasol.adapter.dynamodb.documentpath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Using this class a path through a document can be expressed.
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
     * Creates a sub path starting at startIndex until endIndex.
     * 
     * @param startIndex index in path for new path to start
     * @param endIndex   index in path for new path to end
     * @return new {@link DocumentPathExpression}
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
         * Finishes build process of {@link DocumentPathExpression}.
         * 
         * @return build {@link DocumentPathExpression}
         */
        public DocumentPathExpression build() {
            return new DocumentPathExpression(Collections.unmodifiableList(this.path));
        }
    }
}
