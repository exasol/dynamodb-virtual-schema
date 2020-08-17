package com.exasol.adapter.document.documentpath;

import java.util.List;

/**
 * This class converts a {@link DocumentPathExpression} to string like {@code "/books/topics[1]"}.
 */
public class DocumentPathToStringConverter {
    public String convertToString(final DocumentPathExpression pathExpression) {
        final List<PathSegment> path = pathExpression.getSegments();
        if (path.isEmpty()) {
            return "/";
        } else {
            final StringBuilder resultBuilder = new StringBuilder();
            for (final PathSegment pathSegment : path) {
                resultBuilder.append(pathSegmentToString(pathSegment));
            }
            return resultBuilder.toString();
        }
    }

    private String pathSegmentToString(final PathSegment pathSegment) {
        final StringConverterVisitor visitor = new StringConverterVisitor();
        pathSegment.accept(visitor);
        return visitor.getResult();
    }

    private static class StringConverterVisitor implements PathSegmentVisitor {
        private String result;

        @Override
        public void visit(final ObjectLookupPathSegment objectLookupPathSegment) {
            this.result = "/" + objectLookupPathSegment.getLookupKey();
        }

        @Override
        public void visit(final ArrayLookupPathSegment arrayLookupPathSegment) {
            this.result = "[" + arrayLookupPathSegment.getLookupIndex() + "]";
        }

        @Override
        public void visit(final ArrayAllPathSegment arrayAllPathSegment) {
            this.result = "[*]";
        }

        public String getResult() {
            return this.result;
        }
    }
}
