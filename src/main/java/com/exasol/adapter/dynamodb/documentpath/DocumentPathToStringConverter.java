package com.exasol.adapter.dynamodb.documentpath;

import java.util.List;

/**
 * Converter that converts a {@link DocumentPathExpression} to String e.g. /books/author/
 */
public class DocumentPathToStringConverter {
    public String convertToString(final DocumentPathExpression pathExpression) {
        final List<PathSegment> path = pathExpression.getPath();
        if(path.isEmpty()){
            return "/";
        }
        else {
            final StringBuilder resultBuilder = new StringBuilder("");
            for (final PathSegment pathSegment : path) {
                resultBuilder.append(pathSegmentToString(pathSegment));
            }
            return resultBuilder.toString();
        }
    }

    private String pathSegmentToString(final PathSegment pathSegment) {
        final StringConverterVisitor visitor = new StringConverterVisitor();
        pathSegment.accept(visitor);
        return visitor.result;
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
    }
}
