package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcher;
import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcherFactory;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadataFactory;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;

/**
 * This class creates a {@link AbstractDynamodbDocumentFetcher} for a given request. It decides weather a
 * {@link DynamodbQueryDocumentFetcher} or a {@link DynamodbScanDocumentFetcher} is built, depending on the selection of
 * the query.
 */
public class DynamodbDocumentFetcherFactory implements DocumentFetcherFactory<DynamodbNodeVisitor> {
    private final DynamodbTableMetadataFactory tableMetadataFactory;

    /**
     * Creates an instance of {@link DynamodbDocumentFetcherFactory}.
     * 
     * @param dynamodbClient DynamoDB connection used for fetching table metadata
     */
    public DynamodbDocumentFetcherFactory(final AmazonDynamoDB dynamodbClient) {
        this.tableMetadataFactory = new DynamodbTableMetadataFactory(dynamodbClient);
    }

    @Override
    public DocumentFetcher<DynamodbNodeVisitor> buildDocumentFetcherForQuery(
            final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery) {
        final DynamodbTableMetadata tableMetadata = this.tableMetadataFactory
                .buildMetadataForTable(remoteTableQuery.getFromTable().getRemoteName());
        return buildDocumentFetcherForQuery(remoteTableQuery, tableMetadata);
    }

    DocumentFetcher<DynamodbNodeVisitor> buildDocumentFetcherForQuery(
            final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery, final DynamodbTableMetadata tableMetadata) {
        try {
            return new DynamodbQueryDocumentFetcher(remoteTableQuery, tableMetadata);
        } catch (final PlanDoesNotFitException exception) {
            return new DynamodbScanDocumentFetcher(remoteTableQuery);
        }
    }
}
