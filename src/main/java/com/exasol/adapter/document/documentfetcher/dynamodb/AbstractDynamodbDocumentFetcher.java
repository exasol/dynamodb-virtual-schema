package com.exasol.adapter.document.documentfetcher.dynamodb;

import java.util.Iterator;
import java.util.Map;

import com.exasol.adapter.document.connection.ConnectionPropertiesReader;
import com.exasol.adapter.document.documentfetcher.DocumentFetcher;
import com.exasol.adapter.document.documentfetcher.FetchedDocument;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbMap;
import com.exasol.adapter.document.dynamodb.connection.DynamodbConnectionPropertiesReader;
import com.exasol.adapter.document.iterators.*;
import com.exasol.dynamodb.DynamodbConnectionFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * This class is the abstract basis for DynamoDB {@link DocumentFetcher}s.
 */
abstract class AbstractDynamodbDocumentFetcher implements DocumentFetcher {
    private static final long serialVersionUID = 1110930661591665420L;

    @Override
    public CloseableIterator<FetchedDocument> run(final ConnectionPropertiesReader connectionPropertiesReader) {
        final String tableName = getTableName();
        final DynamoDbClient connection = new DynamodbConnectionFactory()
                .getConnection(new DynamodbConnectionPropertiesReader().read(connectionPropertiesReader));
        final CloseableIterator<Map<String, AttributeValue>> dynamodbResults = new CloseableIteratorWrapper<>(
                this.run(connection), connection::close);
        return new TransformingIterator<>(dynamodbResults,
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
