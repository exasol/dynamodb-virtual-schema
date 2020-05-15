package com.exasol.adapter.dynamodb.documentfetcher;

import java.io.Serializable;
import java.util.stream.Stream;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

/**
 * This interface fetches document data from a remote database.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public interface DocumentFetcher<DocumentVisitorType> extends Serializable {
    /**
     * Executes the planed operation.
     *
     * @param connectionInformation for creating a connection to the remote database
     * @return result of the operation.
     */
    public Stream<DocumentNode<DocumentVisitorType>> run(ExaConnectionInformation connectionInformation);
}
