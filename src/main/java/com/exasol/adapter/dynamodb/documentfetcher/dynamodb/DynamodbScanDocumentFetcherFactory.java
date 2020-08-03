package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
            final RemoteTableQuery remoteTableQuery, final int maxNumberOfParallelFetchers) {
        final DynamodbScanDocumentFetcher.Builder scanDocumentFetcherBuilder = DynamodbScanDocumentFetcher.builder();
        scanDocumentFetcherBuilder.tableName(remoteTableQuery.getFromTable().getRemoteName());
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final String filterExpression = new DynamodbFilterExpressionFactory(namePlaceholderMapBuilder,
                valuePlaceholderMapBuilder).buildFilterExpression(remoteTableQuery.getPushDownSelection());
        final Set<DocumentPathExpression> requiredProperties = new RequiredPathExpressionExtractor()
                .getRequiredProperties(remoteTableQuery);
        final String projectionExpression = this.projectionExpressionFactory.build(requiredProperties,
                namePlaceholderMapBuilder);
        scanDocumentFetcherBuilder.expressionAttributeNames(namePlaceholderMapBuilder.getPlaceholderMap());
        scanDocumentFetcherBuilder.expressionAttributeValues(valuePlaceholderMapBuilder.getPlaceholderMap());
        scanDocumentFetcherBuilder.filterExpression(filterExpression);
        scanDocumentFetcherBuilder.projectionExpression(projectionExpression);
        return parallelizeScan(scanDocumentFetcherBuilder, maxNumberOfParallelFetchers);
    }

    private List<DocumentFetcher<DynamodbNodeVisitor>> parallelizeScan(
            final DynamodbScanDocumentFetcher.Builder templateScanBuilder,
            final int maxNumberOfParallelFetchers) {
        templateScanBuilder.totalSegments(maxNumberOfParallelFetchers);
        final List<DocumentFetcher<DynamodbNodeVisitor>> documentFetchers = new ArrayList<>(
                maxNumberOfParallelFetchers);
        for (int segmentCounter = 0; segmentCounter < maxNumberOfParallelFetchers; segmentCounter++) {
            templateScanBuilder.segment(segmentCounter);
            documentFetchers.add(templateScanBuilder.build());
        }
        return documentFetchers;
    }
}
