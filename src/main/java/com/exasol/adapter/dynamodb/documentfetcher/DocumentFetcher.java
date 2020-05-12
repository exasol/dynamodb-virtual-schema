package com.exasol.adapter.dynamodb.documentfetcher;

import java.util.stream.Stream;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;

/**
 * This interface fetches document data from a remote database for a given {@link RemoteTableQuery}.
 */
public interface DocumentFetcher {
    /**
     * Fetches the document data required for the query.
     *
     * @param remoteTableQuery the query to run
     * @return stream of results
     */
    public Stream<DocumentNode<DynamodbNodeVisitor>> fetchDocumentData(
            final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery);

}
