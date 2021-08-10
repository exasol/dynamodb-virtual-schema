package com.exasol.adapter.document.dynamodb;

import com.exasol.adapter.document.QueryPlanner;
import com.exasol.adapter.document.documentfetcher.dynamodb.DynamodbDocumentFetcherFactory;
import com.exasol.adapter.document.queryplan.FetchQueryPlan;
import com.exasol.adapter.document.queryplan.QueryPlan;
import com.exasol.adapter.document.queryplanning.RemoteTableQuery;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * DynamoDB specific {@link QueryPlanner}.
 */
public class DynamodbQueryPlanner implements QueryPlanner {
    private final DynamodbDocumentFetcherFactory documentFetcherFactory;

    public DynamodbQueryPlanner(final DynamoDbClient dynamodbClient) {
        this.documentFetcherFactory = new DynamodbDocumentFetcherFactory(dynamodbClient);
    }

    @Override
    public QueryPlan planQuery(final RemoteTableQuery remoteTableQuery, final int maxNumberOfParallelFetchers) {
        final DynamodbDocumentFetcherFactory.Result documentFetcherPlan = this.documentFetcherFactory
                .buildDocumentFetcherForQuery(remoteTableQuery, maxNumberOfParallelFetchers);
        return new FetchQueryPlan(documentFetcherPlan.getDocumentFetchers(), documentFetcherPlan.getPostSelection());
    }
}
