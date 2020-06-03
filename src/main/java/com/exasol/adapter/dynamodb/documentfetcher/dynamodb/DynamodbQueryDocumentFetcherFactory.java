package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.List;

import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcher;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbPrimaryIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbSecondaryIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;
import com.exasol.adapter.dynamodb.remotetablequery.normalizer.DnfNormalizer;
import com.exasol.adapter.dynamodb.remotetablequery.normalizer.DnfOr;

/**
 * This factory builds {@link DynamodbQueryDocumentFetcher}s for a given query. If the query can't be solved by a single
 * Query operation but using a few, this factory generates multiple operations.
 */
class DynamodbQueryDocumentFetcherFactory {

    /**
     * Build {@link DynamodbQueryDocumentFetcher}s for a given query.
     *
     * @param remoteTableQuery query to build the {@link DynamodbQueryDocumentFetcher}s for
     * @param tableMetadata    DynamoDB key information for selecting an index
     * @return List of {@link DocumentFetcher}s
     */
    public List<DocumentFetcher<DynamodbNodeVisitor>> buildDocumentFetcherForQuery(
            final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery, final DynamodbTableMetadata tableMetadata) {
        final DnfOr<DynamodbNodeVisitor> dnfOr = new DnfNormalizer<DynamodbNodeVisitor>()
                .normalize(remoteTableQuery.getSelection());
        final QueryOperationSelection bestQueryOperationSelection = findMostSelectiveIndexSelection(tableMetadata,
                dnfOr);

        final QueryRequest queryRequest = new QueryRequest(remoteTableQuery.getFromTable().getRemoteName());
        addSelectionToQuery(bestQueryOperationSelection, queryRequest);
        return List.of(new DynamodbQueryDocumentFetcher(queryRequest));
    }

    private QueryOperationSelection findMostSelectiveIndexSelection(final DynamodbTableMetadata tableMetadata,
            final DnfOr<DynamodbNodeVisitor> dnfOr) {
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
            final QueryRequest queryRequest) {
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
        queryRequest.setExpressionAttributeNames(namePlaceholderMapBuilder.getPlaceholderMap());
        queryRequest.setExpressionAttributeValues(valuePlaceholderMapBuilder.getPlaceholderMap());
    }
}
