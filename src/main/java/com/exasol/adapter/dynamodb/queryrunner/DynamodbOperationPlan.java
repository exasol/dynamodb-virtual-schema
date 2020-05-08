package com.exasol.adapter.dynamodb.queryrunner;

import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * This interface represents a DynamoDB operation for fetching data.
 */
interface DynamodbOperationPlan {
    /**
     * Executes the planed operation.
     * 
     * @param client DynamoDB client
     * @return result of the operation.
     */
    public Stream<Map<String, AttributeValue>> run(final AmazonDynamoDB client);
}
