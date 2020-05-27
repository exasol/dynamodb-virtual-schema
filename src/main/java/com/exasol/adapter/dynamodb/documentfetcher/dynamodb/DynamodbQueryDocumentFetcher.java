package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbPrimaryIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbSecondaryIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;
import com.exasol.adapter.dynamodb.remotetablequery.normalizer.DnfNormalizer;
import com.exasol.adapter.dynamodb.remotetablequery.normalizer.DnfOr;

/**
 * This class represents a DynamoDB {@code QUERY} operation.
 */
public class DynamodbQueryDocumentFetcher extends AbstractDynamodbDocumentFetcher {
    private static final long serialVersionUID = -2972732083876517763L;
    private final QueryRequest queryRequest;

    /**
     * Create an a {@link DynamodbQueryDocumentFetcher} if possible for the given query.
     *
     * @param remoteTableQuery query to build the plan for
     * @param tableMetadata    DynamoDB table metadata used for checking the primary key
     * @throws PlanDoesNotFitException if a DynamoDB {@code QUERY} operation can't fetch the data required by the query
     */
    public DynamodbQueryDocumentFetcher(final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery,
            final DynamodbTableMetadata tableMetadata) {
        final DnfOr<DynamodbNodeVisitor> dnfOr = new DnfNormalizer<DynamodbNodeVisitor>()
                .normalize(remoteTableQuery.getSelection());
        final QueryOperationSelection bestQueryOperationSelection = findMostSelectiveIndexSelection(tableMetadata,
                dnfOr);

        this.queryRequest = new QueryRequest(remoteTableQuery.getFromTable().getRemoteName());
        addSelectionToQuery(bestQueryOperationSelection);
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
            throw new PlanDoesNotFitException("Could not find a Query operation plan");
        }
        return bestQueryOperationSelection;
    }

    private void addSelectionToQuery(final QueryOperationSelection bestQueryOperationSelection) {
        if (!(bestQueryOperationSelection.getIndex() instanceof DynamodbPrimaryIndex)) {
            final DynamodbSecondaryIndex secondaryIndex = (DynamodbSecondaryIndex) bestQueryOperationSelection
                    .getIndex();
            this.queryRequest.setIndexName(secondaryIndex.getIndexName());
        }
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final DynamodbFilterExpressionFactory filterExpressionFactory = new DynamodbFilterExpressionFactory(
                namePlaceholderMapBuilder, valuePlaceholderMapBuilder);
        final String keyFilterExpression = filterExpressionFactory
                .buildFilterExpression(bestQueryOperationSelection.getIndexSelectionAsQueryPredicate());
        this.queryRequest.setKeyConditionExpression(keyFilterExpression);
        final String nonKeyFilterExpression = filterExpressionFactory.buildFilterExpression(
                bestQueryOperationSelection.getNonIndexSelection().asQueryPredicate().simplify());
        if (!nonKeyFilterExpression.isEmpty()) {
            this.queryRequest.setFilterExpression(nonKeyFilterExpression);
        }
        this.queryRequest.setExpressionAttributeNames(namePlaceholderMapBuilder.getPlaceholderMap());
        this.queryRequest.setExpressionAttributeValues(valuePlaceholderMapBuilder.getPlaceholderMap());
    }

    QueryRequest getQueryRequest() {
        return this.queryRequest;
    }

    @Override
    public Stream<Map<String, AttributeValue>> run(final AmazonDynamoDB client) {
        return client.query(this.queryRequest).getItems().stream();
    }
}
