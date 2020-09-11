package com.exasol.adapter.document.documentfetcher.dynamodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.exasol.adapter.document.documentfetcher.DocumentFetcher;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.document.documentpath.DocumentPathExpression;
import com.exasol.adapter.document.queryplanning.RemoteTableQuery;
import com.exasol.adapter.document.queryplanning.RequiredPathExpressionExtractor;

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
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final String filterExpression = new DynamodbFilterExpressionFactory(namePlaceholderMapBuilder,
                valuePlaceholderMapBuilder).buildFilterExpression(remoteTableQuery.getPushDownSelection());
        final Set<DocumentPathExpression> requiredProperties = new RequiredPathExpressionExtractor()
                .getRequiredProperties(remoteTableQuery);
        final String projectionExpression = this.projectionExpressionFactory.build(requiredProperties,
                namePlaceholderMapBuilder);
        final DynamodbScanDocumentFetcher.Builder documentFetcherBuilder = DynamodbScanDocumentFetcher.builder()
                .tableName(remoteTableQuery.getFromTable().getRemoteName())
                .expressionAttributeNames(namePlaceholderMapBuilder.getPlaceholderMap())
                .expressionAttributeValues(valuePlaceholderMapBuilder.getPlaceholderMap())
                .filterExpression(filterExpression).projectionExpression(projectionExpression);
        return parallelizeScan(documentFetcherBuilder, maxNumberOfParallelFetchers);
    }

    private List<DocumentFetcher<DynamodbNodeVisitor>> parallelizeScan(
            final DynamodbScanDocumentFetcher.Builder documentFetcherBuilder, final int maxNumberOfParallelFetchers) {
        documentFetcherBuilder.totalSegments(maxNumberOfParallelFetchers);
        final List<DocumentFetcher<DynamodbNodeVisitor>> documentFetchers = new ArrayList<>(
                maxNumberOfParallelFetchers);
        for (int segmentCounter = 0; segmentCounter < maxNumberOfParallelFetchers; segmentCounter++) {
            documentFetcherBuilder.segment(segmentCounter);
            documentFetchers.add(documentFetcherBuilder.build());
        }
        return documentFetchers;
    }
}
