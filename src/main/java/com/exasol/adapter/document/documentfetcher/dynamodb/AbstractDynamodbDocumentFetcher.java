package com.exasol.adapter.document.documentfetcher.dynamodb;

import java.util.Iterator;
import java.util.Map;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.document.documentfetcher.DocumentFetcher;
import com.exasol.adapter.document.documentfetcher.FetchedDocument;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbMap;
import com.exasol.adapter.document.iterators.AfterAllCallbackIterator;
import com.exasol.adapter.document.iterators.TransformingIterator;
import com.exasol.dynamodb.DynamodbConnectionFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * This class is the abstract basis for DynamoDB {@link DocumentFetcher}s.
 */
abstract class AbstractDynamodbDocumentFetcher implements DocumentFetcher {
    private static final long serialVersionUID = 1110930661591665420L;

    @Override
    public Iterator<FetchedDocument> run(final ExaConnectionInformation connectionInformation) {
        final String tableName = getTableName();
        final DynamoDbClient connection = new DynamodbConnectionFactory().getConnection(connectionInformation);
        final Iterator<Map<String, AttributeValue>> dynamodbResults = this.run(connection);
        final Iterator<Map<String, AttributeValue>> resultsWithCloseDecorator = new AfterAllCallbackIterator<>(
                dynamodbResults, connection::close);
        return new TransformingIterator<>(resultsWithCloseDecorator,
                dynamodbEntry -> new FetchedDocument(new DynamodbMap(dynamodbEntry), tableName));
    }

    /**
     * Executes the planed operation.
     * 
     * @param client DynamoDB client
     * @return result of the operation.
     */
    protected abstract Iterator<Map<String, AttributeValue>> run(final DynamoDbClient client);

    /**
     * Get the name of the DynamoDB table.
     * 
     * @return name of the DynamoDB table
     */
    protected abstract String getTableName();
}
