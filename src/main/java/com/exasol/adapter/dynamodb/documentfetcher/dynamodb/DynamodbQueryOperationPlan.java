package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbSecondaryIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.remotetablequery.QueryPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;

/**
 * This class represents a DynamoDB {@code QUERY} operation.
 */
public class DynamodbQueryOperationPlan implements DynamodbOperationPlan {
    private final QueryRequest queryRequest;

    /**
     * Creates an a {@link DynamodbQueryOperationPlan} if possible for the given query.
     *
     * @param documentQuery document query to build the plan for
     * @param tableMetadata DynamoDB table metadata used for checking the primary key
     * @throws PlanDoesNotFitException if a DynamoDB {@code QUERY} operation can't fetch the data required by the query
     */
    public DynamodbQueryOperationPlan(final RemoteTableQuery<DynamodbNodeVisitor> documentQuery,
            final DynamodbTableMetadata tableMetadata) {
        final DynamodbIndex mostRestrictedIndex = new DynamodbQueryIndexSelector()
                .findMostRestrictedIndex(documentQuery.getSelection(), tableMetadata.getAllIndexes());
        abortIfNoFittingIndexWasFound(mostRestrictedIndex);
        this.queryRequest = new QueryRequest(documentQuery.getFromTable().getRemoteName());
        setIndexToQuery(mostRestrictedIndex);
        addKeyConditionAndFilterExpression(documentQuery, mostRestrictedIndex);
    }

    private void abortIfNoFittingIndexWasFound(final DynamodbIndex mostRestrictedIndex) {
        if (mostRestrictedIndex == null) {
            throw new PlanDoesNotFitException("Could not find a suitable key for a DynamoDB Query operation. "
                    + "Non of the keys did a equality selection with the partition key. "
                    + "Your can either add such a selection to your query or use a SCAN request.");
        }
    }

    private void setIndexToQuery(final DynamodbIndex mostRestrictedIndex) {
        if (mostRestrictedIndex instanceof DynamodbSecondaryIndex) {
            final DynamodbSecondaryIndex secondaryIndex = (DynamodbSecondaryIndex) mostRestrictedIndex;
            this.queryRequest.setIndexName(secondaryIndex.getIndexName());
        }
    }

    private void addKeyConditionAndFilterExpression(final RemoteTableQuery<DynamodbNodeVisitor> documentQuery,
            final DynamodbIndex mostRestrictedIndex) {
        final QueryPredicate<DynamodbNodeVisitor> selectionOnIndex = new DynamodbQuerySelectionFilter()
                .filter(documentQuery.getSelection(), getIndexPropertyNameWhitelist(mostRestrictedIndex));
        final DynamodbValueListBuilder valueListBuilder = new DynamodbValueListBuilder();
        final DynamodbFilterExpressionFactory filterExpressionFactory = new DynamodbFilterExpressionFactory();
        final String keyConditionExpression = filterExpressionFactory.buildFilterExpression(selectionOnIndex,
                valueListBuilder);
        this.queryRequest.setKeyConditionExpression(keyConditionExpression);
        this.queryRequest.setExpressionAttributeValues(valueListBuilder.getValueMap());
    }

    private List<String> getIndexPropertyNameWhitelist(final DynamodbIndex index) {
        if (index.hasSortKey()) {
            return List.of(index.getPartitionKey(), index.getSortKey());
        } else {
            return List.of(index.getPartitionKey());
        }
    }

    QueryRequest getQueryRequest() {
        return this.queryRequest;
    }

    @Override
    public Stream<Map<String, AttributeValue>> run(final AmazonDynamoDB client) {
        return client.query(this.queryRequest).getItems().stream();
    }
}
