package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;

/**
 * This class represents a DynamoDB {@code SCAN} operation.
 */
class DynamodbScanOperationPlan implements DynamodbOperationPlan {
    private final ScanRequest scanRequest;

    /**
     * Creates an instance of {@link DynamodbScanOperationPlan}.
     * 
     * @param tableName table to scan
     */
    protected DynamodbScanOperationPlan(final String tableName) {
        this.scanRequest = new ScanRequest().withTableName(tableName);
    }

    ScanRequest getScanRequest() {
        return this.scanRequest;
    }

    @Override
    public Stream<Map<String, AttributeValue>> run(final AmazonDynamoDB client) {
        return client.scan(this.scanRequest).getItems().stream();
    }
}
