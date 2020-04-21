package com.exasol.adapter.dynamodb.queryrunner;

import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.queryresultschema.QueryResultTableSchema;
import com.exasol.adapter.sql.SqlStatement;

class DynamodbQueryPlanner {
    public DynamodbQueryPlan planQuery(final QueryResultTableSchema schemaMappingPlan, final SqlStatement query,
            final DynamodbTableMetadata tableMetadata) {
        final String tableName = schemaMappingPlan.getFromTable().getRemoteName();
        try {
            return new DynamodbGetItemQueryPlanFactory().buildGetItemPlanIfPossible(tableName, query, tableMetadata);

        } catch (final PlanDoesNotFitException e) {
            return new DynamodbScanQueryPlan(tableName);
        }
    }
}
