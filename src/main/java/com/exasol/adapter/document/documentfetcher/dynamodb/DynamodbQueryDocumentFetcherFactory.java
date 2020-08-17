package com.exasol.adapter.document.documentfetcher.dynamodb;

import java.util.List;
import java.util.Set;

import com.exasol.adapter.document.documentfetcher.DocumentFetcher;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.document.documentpath.DocumentPathExpression;
import com.exasol.adapter.document.dynamodbmetadata.DynamodbIndex;
import com.exasol.adapter.document.dynamodbmetadata.DynamodbPrimaryIndex;
import com.exasol.adapter.document.dynamodbmetadata.DynamodbSecondaryIndex;
import com.exasol.adapter.document.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.document.queryplanning.RemoteTableQuery;
import com.exasol.adapter.document.queryplanning.RequiredPathExpressionExtractor;
import com.exasol.adapter.document.querypredicate.normalizer.DnfNormalizer;
import com.exasol.adapter.document.querypredicate.normalizer.DnfOr;

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
     * @param remoteTableQuery query to build the {@link DynamodbQueryDocumentFetcher}s for
     * @param tableMetadata    DynamoDB key information for selecting an index
     * @return List of {@link DocumentFetcher}s
     */
    public List<DocumentFetcher<DynamodbNodeVisitor>> buildDocumentFetcherForQuery(
            final RemoteTableQuery remoteTableQuery, final DynamodbTableMetadata tableMetadata) {
        final DnfOr dnfOr = new DnfNormalizer().normalize(remoteTableQuery.getPushDownSelection());
        final QueryOperationSelection bestQueryOperationSelection = findMostSelectiveIndexSelection(tableMetadata,
                dnfOr);

        final DynamodbQueryDocumentFetcher.Builder documentFetcherBuilder = DynamodbQueryDocumentFetcher.builder()
                .tableName(remoteTableQuery.getFromTable().getRemoteName());
        addSelectionToQuery(bestQueryOperationSelection, documentFetcherBuilder, remoteTableQuery);
        return List.of(documentFetcherBuilder.build());
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
            throw new PlanDoesNotFitException("Could not find any Query operation plan");
        }
        return bestQueryOperationSelection;
    }

    private void addSelectionToQuery(final QueryOperationSelection bestQueryOperationSelection,
            final DynamodbQueryDocumentFetcher.Builder documentFetcherBuilder,
            final RemoteTableQuery remoteTableQuery) {
        if (!(bestQueryOperationSelection.getIndex() instanceof DynamodbPrimaryIndex)) {
            final DynamodbSecondaryIndex secondaryIndex = (DynamodbSecondaryIndex) bestQueryOperationSelection
                    .getIndex();
            documentFetcherBuilder.indexName(secondaryIndex.getIndexName());
        }
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final DynamodbFilterExpressionFactory filterExpressionFactory = new DynamodbFilterExpressionFactory(
                namePlaceholderMapBuilder, valuePlaceholderMapBuilder);
        final String keyFilterExpression = filterExpressionFactory
                .buildFilterExpression(bestQueryOperationSelection.getIndexSelectionAsQueryPredicate());
        final String nonKeyFilterExpression = filterExpressionFactory.buildFilterExpression(
                bestQueryOperationSelection.getNonIndexSelection().asQueryPredicate().simplify());
        final Set<DocumentPathExpression> requiredProperties = new RequiredPathExpressionExtractor()
                .getRequiredProperties(remoteTableQuery);
        final String projectionExpression = this.projectionExpressionFactory.build(requiredProperties,
                namePlaceholderMapBuilder);
        documentFetcherBuilder.keyConditionExpression(keyFilterExpression).filterExpression(nonKeyFilterExpression)
                .projectionExpression(projectionExpression)
                .expressionAttributeNames(namePlaceholderMapBuilder.getPlaceholderMap())
                .expressionAttributeValues(valuePlaceholderMapBuilder.getPlaceholderMap());
    }
}
