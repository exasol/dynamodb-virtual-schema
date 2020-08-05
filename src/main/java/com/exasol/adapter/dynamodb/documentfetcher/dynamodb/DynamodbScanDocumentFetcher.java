package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * This class represents a DynamoDB {@code SCAN} operation.
 */
class DynamodbScanDocumentFetcher extends AbstractDynamodbDocumentFetcher {
    private static final long serialVersionUID = -4833355898811576425L;//
    private final GenericTableAccessParameters genericParameters;
    private final int totalSegments;
    private final int segment;

    /**
     * Create an instance of {@link DynamodbScanDocumentFetcher}.
     * 
     * @param genericParameters generic parameters (not scan specific)
     * @param totalSegments     number of segments for the parallel scan
     * @param segment           segment to scan
     */
    private DynamodbScanDocumentFetcher(final GenericTableAccessParameters genericParameters, final int totalSegments,
            final int segment) {
        this.genericParameters = genericParameters;
        this.totalSegments = totalSegments;
        this.segment = segment;
    }

    static Builder builder() {
        return new Builder();
    }

    ScanRequest getScanRequest() {
        final ScanRequest.Builder builder = ScanRequest.builder().tableName(this.genericParameters.getTableName());
        if (this.genericParameters.hasExpressionAttributeNames()) {
            builder.expressionAttributeNames(this.genericParameters.getExpressionAttributeNames());
        }
        if (this.genericParameters.hasFilterExpression()) {
            builder.filterExpression(this.genericParameters.getFilterExpression());
        }
        if (this.genericParameters.hasExpressionAttributeValues()) {
            builder.expressionAttributeValues(this.genericParameters.getExpressionAttributeValues());
        }
        if (this.genericParameters.hasProjectionExpression()) {
            builder.projectionExpression(this.genericParameters.getProjectionExpression());
        }
        builder.totalSegments(this.totalSegments).segment(this.segment);
        return builder.build();
    }

    @Override
    public Stream<Map<String, AttributeValue>> run(final DynamoDbClient client) {
        return StreamSupport.stream(new ScannerFactory(client, this.getScanRequest()).spliterator(), false);
    }

    private static class ScannerFactory implements Iterable<Map<String, AttributeValue>> {
        private final DynamoDbClient client;
        private final ScanRequest scanRequest;

        private ScannerFactory(final DynamoDbClient client, final ScanRequest scanRequest) {
            this.client = client;
            this.scanRequest = scanRequest;
        }

        @Override
        public Iterator<Map<String, AttributeValue>> iterator() {
            return new Scanner(this.client, this.scanRequest.toBuilder());
        }
    }

    // TODO use client.scanPaginator instead
    private static class Scanner implements Iterator<Map<String, AttributeValue>> {
        private final DynamoDbClient client;
        private final ScanRequest.Builder scanRequest;
        private Map<String, AttributeValue> lastEvaluatedKey;
        private Iterator<Map<String, AttributeValue>> itemsIterator;

        public Scanner(final DynamoDbClient client, final ScanRequest.Builder scanRequest) {
            this.client = client;
            this.scanRequest = scanRequest;
            runScan();
        }

        private void runScan() {
            final ScanResponse scanResponse = this.client.scan(this.scanRequest.build());
            this.itemsIterator = scanResponse.items().iterator();
            this.lastEvaluatedKey = scanResponse.lastEvaluatedKey();
        }

        @Override
        public boolean hasNext() {
            if (this.itemsIterator.hasNext()) {
                return true;
            } else if (this.lastEvaluatedKey.isEmpty()) {
                return false;
            } else {
                this.scanRequest.exclusiveStartKey(this.lastEvaluatedKey);
                runScan();
                return hasNext();
            }
        }

        @Override
        public Map<String, AttributeValue> next() {
            return this.itemsIterator.next();
        }
    }

    /**
     * Builder for {@link DynamodbScanDocumentFetcher}.
     */
    public static final class Builder {
        private String tableName;
        private Map<String, String> expressionAttributeNames;
        private Map<String, DocumentValue<DynamodbNodeVisitor>> expressionAttributeValues;
        private String filterExpression;
        private String projectionExpression;
        private int totalSegments;
        private int segment;

        private Builder() {
        }

        /**
         * Set the name of the table to scan.
         *
         * @param tableName name of the table
         * @return self
         */
        public Builder tableName(final String tableName) {
            this.tableName = tableName;
            return this;
        }

        /**
         * Set the placeholder map for attribute names.
         *
         * @param expressionAttributeNames placeholder map for attribute names
         * @return self
         */
        public Builder expressionAttributeNames(final Map<String, String> expressionAttributeNames) {
            this.expressionAttributeNames = expressionAttributeNames;
            return this;
        }

        /**
         * Set the placeholder map for attribute values.
         *
         * @param expressionAttributeValues placeholder map for attribute values
         * @return self
         */
        public Builder expressionAttributeValues(
                final Map<String, DocumentValue<DynamodbNodeVisitor>> expressionAttributeValues) {
            this.expressionAttributeValues = expressionAttributeValues;
            return this;
        }

        /**
         * Set the filter expression
         *
         * @param filterExpression filter expression
         * @return self
         */
        public Builder filterExpression(final String filterExpression) {
            this.filterExpression = filterExpression;
            return this;
        }

        /**
         * Set the projection expression.
         *
         * @param projectionExpression projection expression
         * @return self
         */
        public Builder projectionExpression(final String projectionExpression) {
            this.projectionExpression = projectionExpression;
            return this;
        }

        /**
         * Set the number of total segments for the parallel scan.
         *
         * @param totalSegments total number of segments
         * @return self
         */
        public Builder totalSegments(final int totalSegments) {
            this.totalSegments = totalSegments;
            return this;
        }

        /**
         * Set the segment to scan.
         *
         * @param segment segment to scan
         * @return self
         */
        public Builder segment(final int segment) {
            this.segment = segment;
            return this;
        }

        /**
         * Build the {@link DynamodbScanDocumentFetcher}.
         *
         * @return {@link DynamodbScanDocumentFetcher}
         */
        public DynamodbScanDocumentFetcher build() {
            final GenericTableAccessParameters genericTableAccessParameters = new GenericTableAccessParameters(
                    this.tableName, this.expressionAttributeNames, this.expressionAttributeValues,
                    this.filterExpression, this.projectionExpression);
            return new DynamodbScanDocumentFetcher(genericTableAccessParameters, this.totalSegments, this.segment);
        }
    }
}
