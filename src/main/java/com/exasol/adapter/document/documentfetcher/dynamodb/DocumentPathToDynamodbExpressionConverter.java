package com.exasol.adapter.document.documentfetcher.dynamodb;

import com.exasol.adapter.document.documentpath.*;

/**
 * This class builds DynamoDB path expressions
 * {@see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html#Expressions.Attributes.NestedAttributes}
 * from a {@link DocumentPathExpression}.
 */
public class DocumentPathToDynamodbExpressionConverter {
    private static final DocumentPathToDynamodbExpressionConverter INSTANCE = new DocumentPathToDynamodbExpressionConverter();

    /**
     * Empty constructor to hide the public default.
     */
    private DocumentPathToDynamodbExpressionConverter() {
        // empty on purpose.
    }

    /**
     * Get a singleton instance of {@link DocumentPathToDynamodbExpressionConverter}.
     *
     * @return instance of {@link DocumentPathToDynamodbExpressionConverter}
     */
    public static DocumentPathToDynamodbExpressionConverter getInstance() {
        return INSTANCE;
    }

    /**
     * Converts the given {@link DocumentPathExpression} into a DynamoDB path expression.
     * 
     * @param pathToConvert path to be converted
     * @return DynamoDB path expression
     */
    public String convert(final DocumentPathExpression pathToConvert,
            final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder) {
        final StringBuilder dynamodbPathExpressionBuilder = new StringBuilder();
        boolean isFirst = true;
        for (final PathSegment segment : pathToConvert.getSegments()) {
            final SegmentConvertVisitor visitor = new SegmentConvertVisitor(isFirst, namePlaceholderMapBuilder);
            segment.accept(visitor);
            dynamodbPathExpressionBuilder.append(visitor.getPathExpression());
            isFirst = false;
        }
        return dynamodbPathExpressionBuilder.toString();
    }

    private static class SegmentConvertVisitor implements PathSegmentVisitor {
        private final boolean isFirstSegment;
        private String pathExpression;
        private final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder;

        private SegmentConvertVisitor(final boolean isFirstSegment,
                final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder) {
            this.isFirstSegment = isFirstSegment;
            this.namePlaceholderMapBuilder = namePlaceholderMapBuilder;
        }

        @Override
        public void visit(final ObjectLookupPathSegment objectLookupPathSegment) {
            this.pathExpression = (this.isFirstSegment ? "" : ".")
                    + this.namePlaceholderMapBuilder.addValue(objectLookupPathSegment.getLookupKey());
        }

        @Override
        public void visit(final ArrayLookupPathSegment arrayLookupPathSegment) {
            this.pathExpression = "[" + arrayLookupPathSegment.getLookupIndex() + "]";
        }

        @Override
        public void visit(final ArrayAllPathSegment arrayAllPathSegment) {
            throw new UnsupportedOperationException(
                    "ArrayAll path segments can't be converted to DynamoDB path expressions.");
        }

        public String getPathExpression() {
            return this.pathExpression;
        }
    }
}
