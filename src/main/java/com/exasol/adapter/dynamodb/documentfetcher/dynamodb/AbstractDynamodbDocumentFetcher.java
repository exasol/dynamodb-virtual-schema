package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcher;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbMap;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.dynamodb.DynamodbConnectionFactory;

/**
 * This class is the abstract basis for DynamoDB {@link DocumentFetcher}s.
 */
abstract class AbstractDynamodbDocumentFetcher implements DocumentFetcher<DynamodbNodeVisitor> {
    private static final long serialVersionUID = -2141915513393410561L;

    @Override
    public Stream<DocumentNode<DynamodbNodeVisitor>> run(final ExaConnectionInformation connectionInformation) {
        return this.run(new DynamodbConnectionFactory().getLowLevelConnection(connectionInformation))
                .map(DynamodbMap::new);
    }

    /**
     * Executes the planed operation.
     * 
     * @param client DynamoDB client
     * @return result of the operation.
     */
    protected abstract Stream<Map<String, AttributeValue>> run(final AmazonDynamoDB client);
}
