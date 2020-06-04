package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;

/**
 * This class represents a DynamoDB {@code SCAN} operation.
 */
class DynamodbScanDocumentFetcher extends AbstractDynamodbDocumentFetcher {
    private static final long serialVersionUID = -2402720048900542839L;
    private final ScanRequest scanRequest;

    /**
     * Create an instance of {@link DynamodbScanDocumentFetcher}.
     *
     * @param scanRequest DynamoDB scan request
     */
    DynamodbScanDocumentFetcher(final ScanRequest scanRequest) {
        this.scanRequest = scanRequest;
    }

    ScanRequest getScanRequest() {
        return this.scanRequest;
    }

    @Override
    public Stream<Map<String, AttributeValue>> run(final AmazonDynamoDB client) {
        return client.scan(this.scanRequest).getItems().stream();
    }
}
