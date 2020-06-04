package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;

/**
 * This class represents a DynamoDB {@code QUERY} operation.
 */
public class DynamodbQueryDocumentFetcher extends AbstractDynamodbDocumentFetcher {
    private static final long serialVersionUID = -2972732083876517763L;
    private final QueryRequest queryRequest;

    /**
     * Create an a {@link DynamodbQueryDocumentFetcher}.
     *
     * @param queryRequest DynamoDB request
     */
    DynamodbQueryDocumentFetcher(final QueryRequest queryRequest) {
        this.queryRequest = queryRequest;
    }

    QueryRequest getQueryRequest() {
        return this.queryRequest;
    }

    @Override
    public Stream<Map<String, AttributeValue>> run(final AmazonDynamoDB client) {
        return client.query(this.queryRequest).getItems().stream();
    }
}
