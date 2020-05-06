package com.exasol.adapter.dynamodb.queryrunner;

import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;

/**
 * This class represents a DynamoDB {@code SCAN} operation.
 */
public class DynamodbQueryQueryPlan implements DynamodbQueryPlan {
    private final QueryRequest queryRequest;

    /**
     * Creates an instance of {@link DynamodbQueryQueryPlan}.
     *
     * @param queryRequest DynamoDB query request
     */
    DynamodbQueryQueryPlan(final QueryRequest queryRequest) {
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
