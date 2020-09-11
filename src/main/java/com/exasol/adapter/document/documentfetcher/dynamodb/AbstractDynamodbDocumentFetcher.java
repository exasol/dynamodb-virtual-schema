package com.exasol.adapter.document.documentfetcher.dynamodb;

import java.net.URISyntaxException;
import java.util.Map;
import java.util.stream.Stream;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.document.documentfetcher.DocumentFetcher;
import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbMap;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.dynamodb.DynamodbConnectionFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * This class is the abstract basis for DynamoDB {@link DocumentFetcher}s.
 */
abstract class AbstractDynamodbDocumentFetcher implements DocumentFetcher<DynamodbNodeVisitor> {
    private static final long serialVersionUID = -2141915513393410561L;

    @Override
    public Stream<DocumentNode<DynamodbNodeVisitor>> run(final ExaConnectionInformation connectionInformation) {
        try {
            return this.run(new DynamodbConnectionFactory().getConnection(connectionInformation)).map(DynamodbMap::new);
        } catch (final URISyntaxException exception) {
            throw new IllegalStateException("Failed to load data from DynamoDB. Cause: " + exception.getMessage(),
                    exception);
        }
    }

    /**
     * Executes the planed operation.
     * 
     * @param client DynamoDB client
     * @return result of the operation.
     */
    protected abstract Stream<Map<String, AttributeValue>> run(final DynamoDbClient client);
}
