package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

/**
 * This class represents a DynamoDB {@code SCAN} operation.
 */
class DynamodbScanDocumentFetcher extends AbstractDynamodbDocumentFetcher {
    private static final long serialVersionUID = 8061635540586030441L;
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
        return StreamSupport.stream(new ScannerFactory(client, this.scanRequest).spliterator(), false);
    }

    private static class ScannerFactory implements Iterable<Map<String, AttributeValue>> {
        private final AmazonDynamoDB client;
        private final ScanRequest scanRequest;

        private ScannerFactory(final AmazonDynamoDB client, final ScanRequest scanRequest) {
            this.client = client;
            this.scanRequest = scanRequest;
        }

        @Override
        public Iterator<Map<String, AttributeValue>> iterator() {
            return new Scanner(this.client, this.scanRequest.clone());
        }
    }

    private static class Scanner implements Iterator<Map<String, AttributeValue>> {
        private final AmazonDynamoDB client;
        private final ScanRequest scanRequest;
        private Map<String, AttributeValue> lastEvaluatedKey;
        private Iterator<Map<String, AttributeValue>> itemsIterator;

        public Scanner(final AmazonDynamoDB client, final ScanRequest scanRequest) {
            this.client = client;
            this.scanRequest = scanRequest;
            runScan(scanRequest);
        }

        private void runScan(final ScanRequest scanRequest) {
            final ScanResult scanResult = this.client.scan(scanRequest);
            this.itemsIterator = scanResult.getItems().iterator();
            this.lastEvaluatedKey = scanResult.getLastEvaluatedKey();
        }

        @Override
        public boolean hasNext() {
            if (this.itemsIterator.hasNext()) {
                return true;
            } else if (this.lastEvaluatedKey == null) {
                return false;
            } else {
                this.scanRequest.setExclusiveStartKey(this.lastEvaluatedKey);
                runScan(this.scanRequest);
                return hasNext();
            }
        }

        @Override
        public Map<String, AttributeValue> next() {
            return this.itemsIterator.next();
        }
    }
}
