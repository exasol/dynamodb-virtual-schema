package com.exasol.adapter.dynamodb.queryrunner;

import java.util.List;

import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.AbstractDynamodbIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbSecondaryIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.remotetablequery.QueryPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;

/**
 * This class builds a {@link DynamodbQueryQueryPlan} if possible.
 */
public class DynamodbQueryQueryPlanFactory {

    /**
     * Builds a {@link DynamodbQueryQueryPlan} if possible for the given query.
     * 
     * @param documentQuery document query to build the plan for
     * @param tableMetadata DynamoDB table metadata used for checking the primary key
     * @return the generated plan
     */
    public DynamodbQueryQueryPlan buildQueryPlanIfPossible(final RemoteTableQuery<DynamodbNodeVisitor> documentQuery,
            final DynamodbTableMetadata tableMetadata) {
        final AbstractDynamodbIndex mostRestrictedIndex = new DynamodbQueryIndexSelector()
                .findMostRestrictedIndex(documentQuery.getSelection(), tableMetadata.getAllIndexes());
        if (mostRestrictedIndex == null) {
            throw new PlanDoesNotFitException("Could not find a suitable key for a DynamoDB Query operation. "
                    + "Non of the keys did a equality selection with the partition key. "
                    + "Your can either add such a selection to your query or use a SCAN request.");
        }
        final QueryRequest queryRequest = new QueryRequest(documentQuery.getFromTable().getRemoteName());
        if (mostRestrictedIndex instanceof DynamodbSecondaryIndex) {
            final DynamodbSecondaryIndex secondaryIndex = (DynamodbSecondaryIndex) mostRestrictedIndex;
            queryRequest.setIndexName(secondaryIndex.getIndexName());
        }

        final QueryPredicate<DynamodbNodeVisitor> selectionOnIndex = new DynamodbQuerySelectionFilter()
                .filter(documentQuery.getSelection(), getIndexPropertyNameWhitelist(mostRestrictedIndex));
        final DynamodbValueListBuilder valueListBuilder = new DynamodbValueListBuilder();
        final String keyConditionExpression = new DynamodbFilterExpressionFactory()
                .buildFilterExpression(selectionOnIndex, valueListBuilder);
        queryRequest.setKeyConditionExpression(keyConditionExpression);
        queryRequest.setExpressionAttributeValues(valueListBuilder.getValueMap());
        return new DynamodbQueryQueryPlan(queryRequest);
    }

    private List<String> getIndexPropertyNameWhitelist(final AbstractDynamodbIndex index) {
        if (index.hasSortKey()) {
            return List.of(index.getPartitionKey(), index.getSortKey());
        } else {
            return List.of(index.getPartitionKey());
        }
    }
}
