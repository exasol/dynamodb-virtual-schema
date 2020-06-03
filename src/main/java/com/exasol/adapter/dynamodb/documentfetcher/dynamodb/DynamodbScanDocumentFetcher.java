package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;

/**
 * This class represents a DynamoDB {@code SCAN} operation.
 */
class DynamodbScanDocumentFetcher extends AbstractDynamodbDocumentFetcher {
    private static final long serialVersionUID = -2402720048900542839L;
    private final ScanRequest scanRequest;

    /**
     * Create an instance of {@link DynamodbScanDocumentFetcher}.
     *
     * @param documentQuery document query to fetch the documents for
     */
    protected DynamodbScanDocumentFetcher(final RemoteTableQuery<DynamodbNodeVisitor> documentQuery) {
        this.scanRequest = new ScanRequest().withTableName(documentQuery.getFromTable().getRemoteName());
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final String filterExpression = new DynamodbFilterExpressionFactory(namePlaceholderMapBuilder,
                valuePlaceholderMapBuilder).buildFilterExpression(documentQuery.getSelection());
        if (!namePlaceholderMapBuilder.getPlaceholderMap().isEmpty()) {
            this.scanRequest.setExpressionAttributeNames(namePlaceholderMapBuilder.getPlaceholderMap());
        }
        if (!filterExpression.isEmpty()) {
            this.scanRequest.setFilterExpression(filterExpression);
        }
        if (!valuePlaceholderMapBuilder.getPlaceholderMap().isEmpty()) {
            this.scanRequest.setExpressionAttributeValues(valuePlaceholderMapBuilder.getPlaceholderMap());
        }
    }

    ScanRequest getScanRequest() {
        return this.scanRequest;
    }

    @Override
    public Stream<Map<String, AttributeValue>> run(final AmazonDynamoDB client) {
        return client.scan(this.scanRequest).getItems().stream();
    }
}
