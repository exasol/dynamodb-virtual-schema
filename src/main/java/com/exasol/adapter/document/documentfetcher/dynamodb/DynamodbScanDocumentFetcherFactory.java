package com.exasol.adapter.document.documentfetcher.dynamodb;

import java.util.*;
import java.util.stream.Stream;

import com.exasol.adapter.document.documentfetcher.DocumentFetcher;
import com.exasol.adapter.document.documentpath.DocumentPathExpression;
import com.exasol.adapter.document.mapping.ColumnMapping;
import com.exasol.adapter.document.queryplanning.RequiredPathExpressionExtractor;
import com.exasol.adapter.document.querypredicate.QueryPredicate;

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
     * @param tableName                   name of the table to query
     * @param selection                   selection to push down as filter expression to DynamoDB
     * @param maxNumberOfParallelFetchers maximum number of parallel running {@link DocumentFetcher}s
     * @return list of {@link DynamodbScanDocumentFetcher}s
     */
    public List<DocumentFetcher> buildDocumentFetcherForQuery(final String tableName, final QueryPredicate selection,
            final List<ColumnMapping> projection, final int maxNumberOfParallelFetchers) {
        final DynamodbScanDocumentFetcher.Builder documentFetcherBuilder = buildScanRequest(tableName, selection,
                projection.stream());
        return parallelizeScan(documentFetcherBuilder, maxNumberOfParallelFetchers);
    }

    private DynamodbScanDocumentFetcher.Builder buildScanRequest(final String tableName, final QueryPredicate selection,
            final Stream<? extends ColumnMapping> projection) {
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final String filterExpression = new DynamodbFilterExpressionFactory(namePlaceholderMapBuilder,
                valuePlaceholderMapBuilder).buildFilterExpression(selection);
        final Set<DocumentPathExpression> requiredProperties = new RequiredPathExpressionExtractor()
                .getRequiredProperties(projection);
        final String projectionExpression = this.projectionExpressionFactory.build(requiredProperties,
                namePlaceholderMapBuilder);
        return DynamodbScanDocumentFetcher.builder().tableName(tableName)
                .expressionAttributeNames(namePlaceholderMapBuilder.getPlaceholderMap())
                .expressionAttributeValues(valuePlaceholderMapBuilder.getPlaceholderMap())
                .filterExpression(filterExpression).projectionExpression(projectionExpression);
    }

    private List<DocumentFetcher> parallelizeScan(final DynamodbScanDocumentFetcher.Builder documentFetcherBuilder,
            final int maxNumberOfParallelFetchers) {
        documentFetcherBuilder.totalSegments(maxNumberOfParallelFetchers);
        final List<DocumentFetcher> documentFetchers = new ArrayList<>(maxNumberOfParallelFetchers);
        for (int segmentCounter = 0; segmentCounter < maxNumberOfParallelFetchers; segmentCounter++) {
            documentFetcherBuilder.segment(segmentCounter);
            documentFetchers.add(documentFetcherBuilder.build());
        }
        return documentFetchers;
    }
}
