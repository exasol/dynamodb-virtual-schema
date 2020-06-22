package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class builds a DynamoDB projection expression for a list of requested properties.
 */
public class DynamodbProjectionExpressionFactory {
    private static final DynamodbProjectionExpressionFactory INSTANCE = new DynamodbProjectionExpressionFactory();

    /**
     * Empty constructor to hide the public default.
     */
    public DynamodbProjectionExpressionFactory() {
        // empty on purpose
    }

    /**
     * Get a singleton instance of {@link DynamodbProjectionExpressionFactory}.
     *
     * @return instance of {@link DynamodbProjectionExpressionFactory}
     */
    public static DynamodbProjectionExpressionFactory getInstance() {
        return INSTANCE;
    }

    /**
     * Builds a DynamoDB projection expression for a list of requested properties.
     * 
     * @param requestedProperties       list of properties that will be expressed
     * @param namePlaceholderMapBuilder placeholder map builder to which the placeholders are added
     * @return DynamoDB projection expression
     */
    public String build(final Collection<DocumentPathExpression> requestedProperties,
            final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder) {
        return requestedProperties.stream().map(this::truncateAtArrayAllPathSegment)
                .map(DocumentPathToDynamodbExpressionConverter.getInstance()::convert).sorted()
                .filter(new RedundantPathRemover()).map(namePlaceholderMapBuilder::addValue)
                .collect(Collectors.joining(", "));
    }

    private DocumentPathExpression truncateAtArrayAllPathSegment(final DocumentPathExpression expression) {
        final int index = expression.indexOfFirstArrayAllSegment();
        if (index == -1) {
            return expression;
        } else {
            return expression.getSubPath(0, index);
        }
    }

    /**
     * This class removes redundant projection expressions. For example it reduces {@code publisher.name} and
     * {@code publisher} to {@code publisher}, as {@code publisher} includes {@code publisher.name} anyway. DynamoDB
     * requires this reduction, as no overlapping projection expressions are allowed.
     * 
     * This class requires that the stream is sorted ascending.
     */
    private static class RedundantPathRemover implements Predicate<String> {
        private String previousPath = null;

        @Override
        public boolean test(final String path) {
            if (this.previousPath != null && path.startsWith(this.previousPath)) {
                return false;
            } else {
                this.previousPath = path;
                return true;
            }
        }
    }
}
