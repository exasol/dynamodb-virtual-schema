package com.exasol.adapter.document.documentfetcher;

import java.util.List;

import com.exasol.adapter.document.queryplanning.RemoteTableQuery;

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
     * @param remoteTableQuery            the document query build the {@link DocumentFetcher} for
     * @param maxNumberOfParallelFetchers the maximum amount of {@link DocumentFetcher}s that can be used in parallel
     * @return {@link DocumentFetcher}
     */
    public List<DocumentFetcher<DocumentVisitorType>> buildDocumentFetcherForQuery(
            final RemoteTableQuery remoteTableQuery, int maxNumberOfParallelFetchers);
}
