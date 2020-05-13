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
class DynamodbScanOperationPlan implements DynamodbOperationPlan {
    private final ScanRequest scanRequest;

    /**
     * Creates an instance of {@link DynamodbScanOperationPlan}.
     *
     * @param documentQuery document query to fetch the documents for
     */
    protected DynamodbScanOperationPlan(final RemoteTableQuery<DynamodbNodeVisitor> documentQuery) {
        this.scanRequest = new ScanRequest().withTableName(documentQuery.getFromTable().getRemoteName());
        final DynamodbValueListBuilder valueListBuilder = new DynamodbValueListBuilder();
        final String filterExpression = new DynamodbFilterExpressionFactory()
                .buildFilterExpression(documentQuery.getSelection(), valueListBuilder);
        if (!filterExpression.isEmpty()) {
            this.scanRequest.setFilterExpression(filterExpression);
        }
        if (!valueListBuilder.getValueMap().isEmpty()) {
            this.scanRequest.setExpressionAttributeValues(valueListBuilder.getValueMap());
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
