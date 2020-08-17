package com.exasol.adapter.document.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.adapter.document.documentpath.DocumentPathExpression;
import com.exasol.adapter.document.documentpath.DocumentPathIteratorFactory;
import com.exasol.adapter.document.documentpath.PathIterationStateProvider;
import com.exasol.adapter.document.queryplanning.RemoteTableQuery;
import com.exasol.sql.expression.ValueExpression;

/**
 * This call maps document data to Exasol rows according to {@link SchemaMappingQuery}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class SchemaMapper<DocumentVisitorType> {
    private final SchemaMappingQuery query;
    private final ColumnValueExtractorFactory<DocumentVisitorType> columnValueExtractorFactory;

    /**
     * Create a new {@link SchemaMapper} for the given query.
     *
     * @param query                                 query used as plan for the schema mapping
     * @param propertyToColumnValueExtractorFactory factory for value mapper corresponding to
     *                                              {@link DocumentVisitorType}
     */
    public SchemaMapper(final RemoteTableQuery query,
            final PropertyToColumnValueExtractorFactory<DocumentVisitorType> propertyToColumnValueExtractorFactory) {
        this.query = query;
        this.columnValueExtractorFactory = new ColumnValueExtractorFactory<>(propertyToColumnValueExtractorFactory);
    }

    /**
     * Processes a document according to the given schema definition and gives an Exasol result row. If a non-root table
     * is queried multiple results can be returned.
     *
     * @param document document to map
     * @return stream of exasol rows
     */
    public Stream<List<ValueExpression>> mapRow(final DocumentNode<DocumentVisitorType> document) {
        final DocumentPathExpression pathToNestedTable = this.query.getFromTable().getPathInRemoteTable();
        final DocumentPathIteratorFactory<DocumentVisitorType> arrayAllCombinationIterable = new DocumentPathIteratorFactory<>(
                pathToNestedTable, document);
        return arrayAllCombinationIterable.stream().map(iterationState -> mapColumns(document, iterationState));
    }

    private List<ValueExpression> mapColumns(final DocumentNode<DocumentVisitorType> document,
            final PathIterationStateProvider arrayAllIterationState) {
        final List<ValueExpression> resultValues = new ArrayList<>(this.query.getRequiredColumns().size());
        for (final ColumnMapping resultColumn : this.query.getRequiredColumns()) {
            final ColumnValueExtractor<DocumentVisitorType> columnValueExtractor = this.columnValueExtractorFactory
                    .getValueExtractorForColumn(resultColumn);
            final ValueExpression result = columnValueExtractor.extractColumnValue(document, arrayAllIterationState);
            resultValues.add(result);
        }
        return resultValues;
    }
}
