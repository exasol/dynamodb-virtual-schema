package com.exasol.adapter.document.documentfetcher.dynamodb;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import com.exasol.adapter.document.documentfetcher.DocumentFetcher;
import com.exasol.adapter.document.documentpath.DocumentPathExpression;
import com.exasol.adapter.document.dynamodbmetadata.*;
import com.exasol.adapter.document.mapping.ColumnMapping;
import com.exasol.adapter.document.queryplanning.RequiredPathExpressionExtractor;
import com.exasol.adapter.document.querypredicate.normalizer.DnfOr;
import com.exasol.errorreporting.ExaError;

/**
 * This factory builds {@link DynamodbQueryDocumentFetcher}s for a given query. If the query can't be solved by a single
 * {@code Query} operation (see https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html) but using
 * a few, this factory generates multiple operations.
 */
class DynamodbQueryDocumentFetcherFactory {
    private final DynamodbProjectionExpressionFactory projectionExpressionFactory;

    /**
     * Create an instance of {@link DynamodbQueryDocumentFetcherFactory}
     */
    DynamodbQueryDocumentFetcherFactory() {
        this.projectionExpressionFactory = new DynamodbProjectionExpressionFactory();
    }

    /**
     * Build {@link DynamodbQueryDocumentFetcher}s for a given query.
     *
     * @param tableName         name of the table to query
     * @param tableMetadata     DynamoDB key information for selecting an index
     * @param pushDownSelection selection to push down to DynamoDB
     * @param projection        projection
     * @return List of {@link DocumentFetcher}s
     */
    public List<DocumentFetcher> buildDocumentFetcherForQuery(final String tableName,
            final DynamodbTableMetadata tableMetadata, final DnfOr pushDownSelection,
            final List<ColumnMapping> projection) {
        final QueryOperationSelection selection = findMostSelectiveIndexSelection(tableMetadata, pushDownSelection);
        final DynamodbQueryDocumentFetcher documentFetcher = buildQueryDocumentFetcher(tableName, selection,
                projection.stream());
        return List.of(documentFetcher);
    }

    private QueryOperationSelection findMostSelectiveIndexSelection(final DynamodbTableMetadata tableMetadata,
            final DnfOr dnfOr) {
        QueryOperationSelection bestQueryOperationSelection = null;
        int bestRating = -1;
        final QueryOperationSelectionRater selectionRater = new QueryOperationSelectionRater();
        for (final DynamodbIndex index : tableMetadata.getAllIndexes()) {
            try {
                final QueryOperationSelection queryOperationSelection = new QueryOperationSelectionFactory()
                        .build(dnfOr, index);
                final int rating = selectionRater.rate(queryOperationSelection);
                if (rating > bestRating) {
                    bestRating = rating;
                    bestQueryOperationSelection = queryOperationSelection;
                }
            } catch (final PlanDoesNotFitException exception) {
                // in that case we just don't add the plan
            }
        }
        if (bestQueryOperationSelection == null) {
            throw new PlanDoesNotFitException(ExaError.messageBuilder("E-VS-DY-14")
                    .message("Could not find any Query operation plan").toString());
        }
        return bestQueryOperationSelection;
    }

    private DynamodbQueryDocumentFetcher buildQueryDocumentFetcher(final String tableName,
            final QueryOperationSelection selection, final Stream<? extends ColumnMapping> projection) {
        final DynamodbQueryDocumentFetcher.Builder documentFetcherBuilder = DynamodbQueryDocumentFetcher.builder()
                .tableName(tableName);
        if (!(selection.getIndex() instanceof DynamodbPrimaryIndex)) {
            final DynamodbSecondaryIndex secondaryIndex = (DynamodbSecondaryIndex) selection.getIndex();
            documentFetcherBuilder.indexName(secondaryIndex.getIndexName());
        }
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final DynamodbFilterExpressionFactory filterExpressionFactory = new DynamodbFilterExpressionFactory(
                namePlaceholderMapBuilder, valuePlaceholderMapBuilder);
        final String keyFilterExpression = filterExpressionFactory
                .buildFilterExpression(selection.getIndexSelectionAsQueryPredicate());
        final String nonKeyFilterExpression = filterExpressionFactory
                .buildFilterExpression(selection.getNonIndexSelection().asQueryPredicate().simplify());
        final Set<DocumentPathExpression> projectionPaths = new RequiredPathExpressionExtractor()
                .getRequiredProperties(projection);
        final String projectionExpression = this.projectionExpressionFactory.build(projectionPaths,
                namePlaceholderMapBuilder);
        documentFetcherBuilder.keyConditionExpression(keyFilterExpression).filterExpression(nonKeyFilterExpression)
                .projectionExpression(projectionExpression)
                .expressionAttributeNames(namePlaceholderMapBuilder.getPlaceholderMap())
                .expressionAttributeValues(valuePlaceholderMapBuilder.getPlaceholderMap());
        return documentFetcherBuilder.build();
    }
}
