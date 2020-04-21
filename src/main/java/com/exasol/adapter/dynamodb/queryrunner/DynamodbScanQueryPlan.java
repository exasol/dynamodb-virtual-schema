package com.exasol.adapter.dynamodb.queryrunner;

import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;

class DynamodbScanQueryPlan implements DynamodbQueryPlan {
    private final ScanRequest scanRequest;

    protected DynamodbScanQueryPlan(final String tableName) {
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
