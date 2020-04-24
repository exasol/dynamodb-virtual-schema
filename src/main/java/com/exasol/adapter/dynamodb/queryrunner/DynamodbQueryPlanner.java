package com.exasol.adapter.dynamodb.queryrunner;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentquery.DocumentQuery;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.sql.SqlStatement;

/**
 * This class creates a {@link DynamodbQueryPlan} for a given request. It decides weather a {@code GetItem},
 * {@code Query} or {@code Scan} operation is performed. The decision depends on the possibility of using a DynamoDB the
 * DynamoDB key or Index for the given query.
 */
class DynamodbQueryPlanner {
    /**
     * Creates a {@link DynamodbQueryPlan} for a given request.
     * 
     * @param schemaMappingPlan planned schema mapping
     * @param query             SQL query
     * @param tableMetadata     DynamoDB table metadata for extracting the key structure
     * @return {@link DynamodbQueryPlan}
     */
    public DynamodbQueryPlan planQuery(final DocumentQuery<DynamodbNodeVisitor> schemaMappingPlan,
            final SqlStatement query, final DynamodbTableMetadata tableMetadata) {
        final String tableName = schemaMappingPlan.getFromTable().getRemoteName();
        return new DynamodbScanQueryPlan(tableName);
    }
}
