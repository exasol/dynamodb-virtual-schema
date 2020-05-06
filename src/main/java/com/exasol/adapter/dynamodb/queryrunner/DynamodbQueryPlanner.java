package com.exasol.adapter.dynamodb.queryrunner;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;

/**
 * This class creates a {@link DynamodbQueryPlan} for a given request. It decides weather a {@code GetItem},
 * {@code Query} or {@code Scan} operation is performed. The decision depends on the possibility of using a DynamoDB the
 * DynamoDB key or Index for the given query.
 */
class DynamodbQueryPlanner {
    /**
     * Creates a {@link DynamodbQueryPlan} for a given request.
     * 
     * @param documentQuery query for this table
     * @param tableMetadata DynamoDB table metadata for extracting the key structure
     * @return {@link DynamodbQueryPlan}
     */
    public DynamodbQueryPlan planQuery(final RemoteTableQuery<DynamodbNodeVisitor> documentQuery,
            final DynamodbTableMetadata tableMetadata) {
        final String tableName = documentQuery.getFromTable().getRemoteName();
        try {
            return new DynamodbQueryQueryPlanFactory().buildQueryPlanIfPossible(documentQuery, tableMetadata);
        } catch (final PlanDoesNotFitException exception) {
            return new DynamodbScanQueryPlan(tableName);
        }
    }
}
