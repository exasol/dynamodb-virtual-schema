package com.exasol.adapter.document.dynamodb;

import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.document.DataLoader;
import com.exasol.adapter.document.DataLoaderFactory;
import com.exasol.adapter.document.documentfetcher.dynamodb.DynamodbDocumentFetcherFactory;
import com.exasol.adapter.document.queryplanning.RemoteTableQuery;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * DynamoDB specific {@link DataLoaderFactory}.
 */
public class DynamodbDataLoaderFactory implements DataLoaderFactory {
    private final DynamodbDocumentFetcherFactory documentFetcherFactory;

    public DynamodbDataLoaderFactory(DynamoDbClient dynamodbClient) {
        this.documentFetcherFactory = new DynamodbDocumentFetcherFactory(dynamodbClient);
    }

    @Override
    public List<DataLoader> buildDataLoaderForQuery(final RemoteTableQuery remoteTableQuery,
            final int maxNumberOfParallelFetchers) {
        return documentFetcherFactory.buildDocumentFetcherForQuery(remoteTableQuery, maxNumberOfParallelFetchers)
                .stream().map(DynamodbDataLoader::new).collect(Collectors.toList());
    }
}
