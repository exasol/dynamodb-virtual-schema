package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.List;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcher;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbPrimaryIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbSecondaryIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.queryplanning.RemoteTableQuery;
import com.exasol.adapter.dynamodb.queryplanning.RequiredPathExpressionExtractor;
import com.exasol.adapter.dynamodb.querypredicate.normalizer.DnfNormalizer;
import com.exasol.adapter.dynamodb.querypredicate.normalizer.DnfOr;

/**
 * This factory builds {@link DynamodbQueryDocumentFetcher}s for a given query. If the query can't be solved by a single
 * {@code Query} operation (see https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html) but using
 * a few, this factory generates multiple operations.
 */
class DynamodbQueryDocumentFetcherFactory {
    private final DynamodbProjectionExpressionFactory projectionExpressionFactory;

    /**
     * Create an instrance of {@link DynamodbQueryDocumentFetcherFactory}
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

        final QueryRequest queryRequest = new QueryRequest(remoteTableQuery.getFromTable().getRemoteName());
        addSelectionToQuery(bestQueryOperationSelection, queryRequest, remoteTableQuery);
        return List.of(new DynamodbQueryDocumentFetcher(queryRequest));
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
                continue;
            }
        }
        if (bestQueryOperationSelection == null) {
            throw new PlanDoesNotFitException("Could not find any Query operation plan");
        }
        return bestQueryOperationSelection;
    }

    private void addSelectionToQuery(final QueryOperationSelection bestQueryOperationSelection,
            final QueryRequest queryRequest, final RemoteTableQuery remoteTableQuery) {
        if (!(bestQueryOperationSelection.getIndex() instanceof DynamodbPrimaryIndex)) {
            final DynamodbSecondaryIndex secondaryIndex = (DynamodbSecondaryIndex) bestQueryOperationSelection
                    .getIndex();
            queryRequest.setIndexName(secondaryIndex.getIndexName());
        }
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final DynamodbFilterExpressionFactory filterExpressionFactory = new DynamodbFilterExpressionFactory(
                namePlaceholderMapBuilder, valuePlaceholderMapBuilder);
        final String keyFilterExpression = filterExpressionFactory
                .buildFilterExpression(bestQueryOperationSelection.getIndexSelectionAsQueryPredicate());
        queryRequest.setKeyConditionExpression(keyFilterExpression);
        final String nonKeyFilterExpression = filterExpressionFactory.buildFilterExpression(
                bestQueryOperationSelection.getNonIndexSelection().asQueryPredicate().simplify());
        if (!nonKeyFilterExpression.isEmpty()) {
            queryRequest.setFilterExpression(nonKeyFilterExpression);
        }
        final Set<DocumentPathExpression> requiredProperties = new RequiredPathExpressionExtractor()
                .getRequiredProperties(remoteTableQuery);
        final String projectionExpression = this.projectionExpressionFactory.build(requiredProperties,
                namePlaceholderMapBuilder);
        if (!projectionExpression.isEmpty()) {
            queryRequest.setProjectionExpression(projectionExpression);
        }
        queryRequest.setExpressionAttributeNames(namePlaceholderMapBuilder.getPlaceholderMap());
        queryRequest.setExpressionAttributeValues(valuePlaceholderMapBuilder.getPlaceholderMap());
    }
}
