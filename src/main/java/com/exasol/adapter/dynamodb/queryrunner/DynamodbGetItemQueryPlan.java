package com.exasol.adapter.dynamodb.queryrunner;

import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;

/**
 * This class represents a DynamoDB {@code SCAN} operation.
 */
public class DynamodbGetItemQueryPlan implements DynamodbQueryPlan {
    private final GetItemRequest getItemRequest;

    /**
     * Creates an instance of {@link DynamodbGetItemQueryPlan}.
     * 
     * @param tableName table to query
     * @param key       Primary key of the item to get
     */
    public DynamodbGetItemQueryPlan(final String tableName, final Map<String, AttributeValue> key) {
        this.getItemRequest = new GetItemRequest().withTableName(tableName).withKey(key);
    }

    @Override
    public Stream<Map<String, AttributeValue>> run(final AmazonDynamoDB client) {
        final Map<String, AttributeValue> item = client.getItem(this.getItemRequest).getItem();
        return Stream.ofNullable(item);
    }

    GetItemRequest getGetItemRequest() {
        return this.getItemRequest;
    }
}
