package com.exasol.adapter.dynamodb.documentfetcher;

import java.util.List;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;

/**
 * This factory builds {@link DocumentFetcher}s that fetch the required documents for a given {@link RemoteTableQuery}.
 * If multiple document fetchers are returned, then the results must be combined by an {@code UNION ALL}. This
 * combination is implicitly implemented by the UDFs as multiple UDFs emit the value. This results in an union of the
 * values without duplicate elimination.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public interface DocumentFetcherFactory<DocumentVisitorType> {
    /**
     * Builds a {@link DocumentFetcher} for a given query
     *
     * @param remoteTableQuery the document query build the {@link DocumentFetcher} for
     * @return {@link DocumentFetcher}
     */
    public List<DocumentFetcher<DocumentVisitorType>> buildDocumentFetcherForQuery(
            final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery);
}
