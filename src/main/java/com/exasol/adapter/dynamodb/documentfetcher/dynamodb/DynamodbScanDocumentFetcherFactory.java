package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcher;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;

/**
 * This factory builds {@link DynamodbScanDocumentFetcher}s for a given query. It returns multiple
 * {@link DocumentFetcher}s so that their execution can be parallelized.
 */
class DynamodbScanDocumentFetcherFactory {

    /**
     * Build {@link DynamodbScanDocumentFetcher}s for a given query.
     * 
     * @param remoteTableQuery query to build the {@link DynamodbScanDocumentFetcher}s for
     * @return list of {@link DynamodbScanDocumentFetcher}s
     */
    public List<DocumentFetcher<DynamodbNodeVisitor>> buildDocumentFetcherForQuery(
            final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery) {
        final ScanRequest templateScanRequest = new ScanRequest()
                .withTableName(remoteTableQuery.getFromTable().getRemoteName());
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final String filterExpression = new DynamodbFilterExpressionFactory(namePlaceholderMapBuilder,
                valuePlaceholderMapBuilder).buildFilterExpression(remoteTableQuery.getSelection());
        if (!namePlaceholderMapBuilder.getPlaceholderMap().isEmpty()) {
            templateScanRequest.setExpressionAttributeNames(namePlaceholderMapBuilder.getPlaceholderMap());
        }
        if (!filterExpression.isEmpty()) {
            templateScanRequest.setFilterExpression(filterExpression);
        }
        if (!valuePlaceholderMapBuilder.getPlaceholderMap().isEmpty()) {
            templateScanRequest.setExpressionAttributeValues(valuePlaceholderMapBuilder.getPlaceholderMap());
        }
        return parallelizeScan(templateScanRequest);
    }

    private List<DocumentFetcher<DynamodbNodeVisitor>> parallelizeScan(final ScanRequest templateScanRequest) {
        final int numberOfSegments = 10; // TODO replace by calculated number
        templateScanRequest.withTotalSegments(numberOfSegments);
        final List<DocumentFetcher<DynamodbNodeVisitor>> documentFetchers = new ArrayList<>(numberOfSegments);
        for (int segmentCounter = 0; segmentCounter < numberOfSegments; segmentCounter++) {
            final ScanRequest scanRequest = templateScanRequest.clone();
            scanRequest.withSegment(segmentCounter);
            documentFetchers.add(new DynamodbScanDocumentFetcher(scanRequest));
        }
        return documentFetchers;
    }
}
