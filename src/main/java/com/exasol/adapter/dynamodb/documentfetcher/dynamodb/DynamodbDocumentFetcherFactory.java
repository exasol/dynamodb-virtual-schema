package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.List;

import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcher;
import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcherFactory;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.BaseDynamodbTableMetadataFactory;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadataFactory;
import com.exasol.adapter.dynamodb.queryplanning.RemoteTableQuery;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * This class creates a {@link AbstractDynamodbDocumentFetcher} for a given request. It decides weather a
 * {@link DynamodbQueryDocumentFetcher} or a {@link DynamodbScanDocumentFetcher} is built, depending on the selection of
 * the query.
 */
public class DynamodbDocumentFetcherFactory implements DocumentFetcherFactory<DynamodbNodeVisitor> {
    private final DynamodbTableMetadataFactory tableMetadataFactory;

    /**
     * Create an instance of {@link DynamodbDocumentFetcherFactory}.
     * 
     * @param dynamodbClient DynamoDB connection used for fetching table metadata
     */
    public DynamodbDocumentFetcherFactory(final DynamoDbClient dynamodbClient) {
        this.tableMetadataFactory = new BaseDynamodbTableMetadataFactory(dynamodbClient);
    }

    @Override
    public List<DocumentFetcher<DynamodbNodeVisitor>> buildDocumentFetcherForQuery(
            final RemoteTableQuery remoteTableQuery, final int maxNumberOfParallelFetchers) {
        final DynamodbTableMetadata tableMetadata = this.tableMetadataFactory
                .buildMetadataForTable(remoteTableQuery.getFromTable().getRemoteName());
        return buildDocumentFetcherForQuery(remoteTableQuery, tableMetadata, maxNumberOfParallelFetchers);
    }

    List<DocumentFetcher<DynamodbNodeVisitor>> buildDocumentFetcherForQuery(final RemoteTableQuery remoteTableQuery,
            final DynamodbTableMetadata tableMetadata, final int maxNumberOfParallelFetchers) {
        try {
            return new DynamodbQueryDocumentFetcherFactory().buildDocumentFetcherForQuery(remoteTableQuery,
                    tableMetadata);
        } catch (final PlanDoesNotFitException exception) {
            return new DynamodbScanDocumentFetcherFactory().buildDocumentFetcherForQuery(remoteTableQuery,
                    maxNumberOfParallelFetchers);
        }
    }
}
