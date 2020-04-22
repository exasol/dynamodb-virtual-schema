package com.exasol.adapter.dynamodb.queryrunner;

import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.queryplan.DocumentQuery;
import com.exasol.adapter.sql.SqlStatement;

class DynamodbQueryPlanner {
    public DynamodbQueryPlan planQuery(final DocumentQuery schemaMappingPlan, final SqlStatement query,
            final DynamodbTableMetadata tableMetadata) {
        final String tableName = schemaMappingPlan.getFromTable().getRemoteName();
        return new DynamodbScanQueryPlan(tableName);
    }
}
