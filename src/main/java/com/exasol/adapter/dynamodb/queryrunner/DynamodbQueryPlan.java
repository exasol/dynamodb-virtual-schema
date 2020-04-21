package com.exasol.adapter.dynamodb.queryrunner;

import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

interface DynamodbQueryPlan {
    public Stream<Map<String, AttributeValue>> run(final AmazonDynamoDB client);
}
