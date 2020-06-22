package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.Collection;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class builds a DynamoDB projection expression for a list of requested properties.
 */
public class DynamodbProjectionExpressionFactory {
    DocumentPathToDynamodbExpressionConverter pathConverter = new DocumentPathToDynamodbExpressionConverter();

    /**
     * Builds a DynamoDB projection expression for a list of requested properties.
     * 
     * @param requestedProperties       list of properties that will be expressed
     * @param namePlaceholderMapBuilder placeholder map builder to which the placeholders are added
     * @return DynamoDB projection expression
     */
    public String build(final Collection<DocumentPathExpression> requestedProperties,
            final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder) {
        return requestedProperties.stream().map(this::truncateAtArrayAllPathSegment).map(this.pathConverter::convert)
                .map(namePlaceholderMapBuilder::addValue).collect(Collectors.joining(", "));
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
