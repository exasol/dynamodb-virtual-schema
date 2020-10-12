package com.exasol.adapter.document.documentfetcher.dynamodb;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.exasol.adapter.document.documentfetcher.DocumentFetcher;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.document.dynamodbmetadata.BaseDynamodbTableMetadataFactory;
import com.exasol.adapter.document.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.document.dynamodbmetadata.DynamodbTableMetadataFactory;
import com.exasol.adapter.document.mapping.ColumnMapping;
import com.exasol.adapter.document.queryplanning.RemoteTableQuery;
import com.exasol.adapter.document.queryplanning.selectionextractor.SelectionExtractor;
import com.exasol.adapter.document.querypredicate.InvolvedColumnCollector;
import com.exasol.adapter.document.querypredicate.QueryPredicate;
import com.exasol.adapter.document.querypredicate.normalizer.DnfOr;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * This class creates a {@link AbstractDynamodbDocumentFetcher} for a given request. It decides weather a
 * {@link DynamodbQueryDocumentFetcher} or a {@link DynamodbScanDocumentFetcher} is built, depending on the selection of
 * the query.
 */
public class DynamodbDocumentFetcherFactory {
    private final DynamodbTableMetadataFactory tableMetadataFactory;

    /**
     * Create an instance of {@link DynamodbDocumentFetcherFactory}.
     * 
     * @param dynamodbClient DynamoDB connection used for fetching table metadata
     */
    public DynamodbDocumentFetcherFactory(final DynamoDbClient dynamodbClient) {
        this.tableMetadataFactory = new BaseDynamodbTableMetadataFactory(dynamodbClient);
    }

    /**
     * Build a DynamoDB {@link DocumentFetcher}.
     * 
     * @param remoteTableQuery            query
     * @param maxNumberOfParallelFetchers maximum number of parallel running fetchers
     * @return {@link Result}.
     */
    public Result buildDocumentFetcherForQuery(
            final RemoteTableQuery remoteTableQuery, final int maxNumberOfParallelFetchers) {
        final DynamodbTableMetadata tableMetadata = this.tableMetadataFactory
                .buildMetadataForTable(remoteTableQuery.getFromTable().getRemoteName());
        return buildDocumentFetcherForQuery(remoteTableQuery, tableMetadata, maxNumberOfParallelFetchers);
    }

    Result buildDocumentFetcherForQuery(final RemoteTableQuery remoteTableQuery,
            final DynamodbTableMetadata tableMetadata, final int maxNumberOfParallelFetchers) {
        final String tableName = remoteTableQuery.getFromTable().getRemoteName();
        final SelectionExtractor selectionExtractor = new SelectionExtractor(
                DynamodbFilterExpressionFactory::canConvert);
        final SelectionExtractor.Result result = selectionExtractor
                .extractIndexColumnSelection(remoteTableQuery.getSelection());
        final DnfOr pushDownSelection = result.getSelectedSelection();
        final List<ColumnMapping> projection = getProjection(remoteTableQuery, result.getRemainingSelection());
        final List<DocumentFetcher<DynamodbNodeVisitor>> documentFetchers = buildDocumentFetchers(tableMetadata,
                maxNumberOfParallelFetchers, tableName, pushDownSelection, projection);
        return new Result(documentFetchers, result.getRemainingSelection().asQueryPredicate());
    }

    private List<DocumentFetcher<DynamodbNodeVisitor>> buildDocumentFetchers(final DynamodbTableMetadata tableMetadata,
            final int maxNumberOfParallelFetchers, final String tableName, final DnfOr pushDownSelection,
            final List<ColumnMapping> projection) {
        try {
            return new DynamodbQueryDocumentFetcherFactory().buildDocumentFetcherForQuery(tableName, tableMetadata,
                    pushDownSelection, projection);
        } catch (final PlanDoesNotFitException exception) {
            return new DynamodbScanDocumentFetcherFactory().buildDocumentFetcherForQuery(tableName,
                    pushDownSelection.asQueryPredicate(), projection, maxNumberOfParallelFetchers);
        }
    }

    private List<ColumnMapping> getProjection(final RemoteTableQuery remoteTableQuery, final DnfOr postSelection) {
        final List<ColumnMapping> columnsRequiredByPostSelection = new InvolvedColumnCollector()
                .collectInvolvedColumns(postSelection.asQueryPredicate());
        return Stream.concat(columnsRequiredByPostSelection.stream(), remoteTableQuery.getSelectList().stream())
                .distinct().sorted(Comparator.comparing(ColumnMapping::getExasolColumnName))
                .collect(Collectors.toList());
    }

    /**
     * Result of the {@link DynamodbFilterExpressionFactory}.
     */
    public static class Result {
        private final List<DocumentFetcher<DynamodbNodeVisitor>> documentFetchers;
        private final QueryPredicate postSelection;

        private Result(final List<DocumentFetcher<DynamodbNodeVisitor>> documentFetchers,
                final QueryPredicate postSelection) {
            this.documentFetchers = documentFetchers;
            this.postSelection = postSelection;
        }

        /**
         * Get the built {@link DocumentFetcher}s.
         * 
         * @return built {@link DocumentFetcher}s
         */
        public List<DocumentFetcher<DynamodbNodeVisitor>> getDocumentFetchers() {
            return this.documentFetchers;
        }

        /**
         * Get the remaining post selection.
         * 
         * @return remaining post selection
         */
        public QueryPredicate getPostSelection() {
            return this.postSelection;
        }
    }
}
