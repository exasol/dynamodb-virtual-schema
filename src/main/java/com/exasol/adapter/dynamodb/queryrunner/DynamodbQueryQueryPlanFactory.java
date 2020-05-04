package com.exasol.adapter.dynamodb.queryrunner;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbIndex;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
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
        final DynamodbIndex mostRestrictedIndex = new DynamodbQueryIndexSelector()
                .findMostRestrictedIndex(documentQuery.getSelection(), tableMetadata.getAllIndexes());
        if (mostRestrictedIndex == null) {
            throw new PlanDoesNotFitException("Could not find a suitable key for a DynamoDB Query operation. "
                    + "Non of the keys did a equality selection with the partition key. "
                    + "Your can either add such a selection to your query or use a SCAN request.");
        }
        // TODO continue implementation
        return null;
    }

}
