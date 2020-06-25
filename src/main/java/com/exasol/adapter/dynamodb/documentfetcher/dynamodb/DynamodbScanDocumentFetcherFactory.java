package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcher;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.queryplanning.RemoteTableQuery;
import com.exasol.adapter.dynamodb.queryplanning.RequiredPathExpressionExtractor;

/**
 * This factory builds {@link DynamodbScanDocumentFetcher}s for a given query. It returns multiple
 * {@link DocumentFetcher}s so that their execution can be parallelized.
 */
class DynamodbScanDocumentFetcherFactory {
    private final DynamodbProjectionExpressionFactory projectionExpressionFactory;

    /**
     * Create an instance of {@link DynamodbScanDocumentFetcherFactory}
     */
    public DynamodbScanDocumentFetcherFactory() {
        this.projectionExpressionFactory = new DynamodbProjectionExpressionFactory();
    }

    /**
     * Build {@link DynamodbScanDocumentFetcher}s for a given query.
     * 
     * @param remoteTableQuery query to build the {@link DynamodbScanDocumentFetcher}s for
     * @return list of {@link DynamodbScanDocumentFetcher}s
     */
    public List<DocumentFetcher<DynamodbNodeVisitor>> buildDocumentFetcherForQuery(
            final RemoteTableQuery remoteTableQuery) {
        final ScanRequest templateScanRequest = new ScanRequest()
                .withTableName(remoteTableQuery.getFromTable().getRemoteName());
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final String filterExpression = new DynamodbFilterExpressionFactory(namePlaceholderMapBuilder,
                valuePlaceholderMapBuilder).buildFilterExpression(remoteTableQuery.getPushDownSelection());
        final Set<DocumentPathExpression> requiredProperties = new RequiredPathExpressionExtractor()
                .getRequiredProperties(remoteTableQuery);
        final String projectionExpression = this.projectionExpressionFactory.build(requiredProperties,
                namePlaceholderMapBuilder);
        if (!namePlaceholderMapBuilder.getPlaceholderMap().isEmpty()) {
            templateScanRequest.setExpressionAttributeNames(namePlaceholderMapBuilder.getPlaceholderMap());
        }
        if (!filterExpression.isEmpty()) {
            templateScanRequest.setFilterExpression(filterExpression);
        }
        if (!valuePlaceholderMapBuilder.getPlaceholderMap().isEmpty()) {
            templateScanRequest.setExpressionAttributeValues(valuePlaceholderMapBuilder.getPlaceholderMap());
        }
        if (!projectionExpression.isEmpty()) {
            templateScanRequest.setProjectionExpression(projectionExpression);
        }
        return parallelizeScan(templateScanRequest);
    }

    private List<DocumentFetcher<DynamodbNodeVisitor>> parallelizeScan(final ScanRequest templateScanRequest) {
        final int numberOfSegments = 16; // TODO replace by calculated number
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
