package com.exasol.adapter.document.documentfetcher.dynamodb;

import java.util.Map;
import java.util.stream.Stream;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.document.documentfetcher.DocumentFetcher;
import com.exasol.adapter.document.documentfetcher.FetchedDocument;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbMap;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.dynamodb.DynamodbConnectionFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * This class is the abstract basis for DynamoDB {@link DocumentFetcher}s.
 */
abstract class AbstractDynamodbDocumentFetcher implements DocumentFetcher<DynamodbNodeVisitor> {
    private static final long serialVersionUID = 1110930661591665420L;

    @Override
    public Stream<FetchedDocument<DynamodbNodeVisitor>> run(final ExaConnectionInformation connectionInformation) {
        final String tableName = getTableName();
        return this.run(new DynamodbConnectionFactory().getConnection(connectionInformation)).map(DynamodbMap::new)
                .map(document -> new FetchedDocument<>(document, tableName));
    }

    /**
     * Executes the planed operation.
     * 
     * @param client DynamoDB client
     * @return result of the operation.
     */
    protected abstract Stream<Map<String, AttributeValue>> run(final DynamoDbClient client);

    /**
     * Get the name of the DynamoDB table.
     * 
     * @return name of the DynamoDB table
     */
    protected abstract String getTableName();
}
