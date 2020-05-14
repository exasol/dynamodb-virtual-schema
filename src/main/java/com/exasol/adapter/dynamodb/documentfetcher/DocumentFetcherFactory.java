package com.exasol.adapter.dynamodb.documentfetcher;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;

/**
 * This factory builds {@link DocumentFetcher}s that fetch the required documents for a given {@link RemoteTableQuery}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public interface DocumentFetcherFactory<DocumentVisitorType> {
    /**
     * Builds a {@link DocumentFetcher} for a given query
     *
     * @param remoteTableQuery      the document query build the {@link DocumentFetcher} for
     * @param connectionInformation connection information for connecting the remote database for fetching metadata,
     *                              used for query planning
     * @return {@link DocumentFetcher}
     */
    public DocumentFetcher<DocumentVisitorType> buildDocumentFetcherForQuery(
            final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery,
            ExaConnectionInformation connectionInformation);
}
