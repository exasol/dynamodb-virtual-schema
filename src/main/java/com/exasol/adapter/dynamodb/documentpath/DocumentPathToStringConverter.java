package com.exasol.adapter.dynamodb.documentpath;

import java.util.List;

/**
 * Converter that converts a {@link DocumentPathExpression} to String e.g. /books/author/
 */
public class DocumentPathToStringConverter {
    public String convertToString(final DocumentPathExpression pathExpression) {
        final List<PathSegment> path = pathExpression.getPath();
        final StringBuilder resultBuilder = new StringBuilder("/");
        for (final PathSegment pathSegment : path) {
            resultBuilder.append(pathSegmentToString(pathSegment));
        }
        return resultBuilder.toString();
    }

    private String pathSegmentToString(final PathSegment pathSegment) {
        final StringConverterVisitor visitor = new StringConverterVisitor();
        pathSegment.accept(visitor);
        return visitor.result;
    }

    private static class StringConverterVisitor implements PathSegmentVisitor {
        private String result;

        @Override
        public void visit(final ObjectPathSegment objectPathSegment) {
            this.result = objectPathSegment.getLookupKey() + "/";
        }
    }
}
