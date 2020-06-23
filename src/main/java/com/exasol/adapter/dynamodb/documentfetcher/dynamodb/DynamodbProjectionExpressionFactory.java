package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.documentpath.RedundantPathEliminator;

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
        final Stream<DocumentPathExpression> paths = requestedProperties.stream()
                .map(this::truncateAtArrayAllPathSegment);
        final Set<DocumentPathExpression> redundancyFreePaths = RedundantPathEliminator.getInstance()
                .removeRedundantPaths(paths);
        return redundancyFreePaths.stream().map(path -> DocumentPathToDynamodbExpressionConverter.getInstance()
                .convert(path, namePlaceholderMapBuilder)).collect(Collectors.joining(", "));
    }

    private DocumentPathExpression truncateAtArrayAllPathSegment(final DocumentPathExpression expression) {
        final int index = expression.indexOfFirstArrayAllSegment();
        if (index == -1) {
            return expression;
        } else {
            return expression.getSubPath(0, index);
        }
    }
}
