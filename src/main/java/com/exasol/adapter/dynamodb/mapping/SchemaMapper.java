package com.exasol.adapter.dynamodb.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathIteratorFactory;
import com.exasol.adapter.dynamodb.documentpath.PathIterationStateProvider;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;
import com.exasol.sql.expression.ValueExpression;

/**
 * This call maps document data to Exasol rows according to {@link SchemaMappingQuery}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class SchemaMapper<DocumentVisitorType> {
    private final SchemaMappingQuery query;
    private final ValueMapperFactory<DocumentVisitorType> valueMapperFactory;

    /**
     * Creates a new {@link SchemaMapper} for the given query.
     *
     * @param query              query used as plan for the schema mapping
     * @param valueMapperFactory factory for value mapper corresponding to {@link DocumentVisitorType}
     */
    public SchemaMapper(final RemoteTableQuery<DocumentVisitorType> query,
            final ValueMapperFactory<DocumentVisitorType> valueMapperFactory) {
        this.query = query;
        this.valueMapperFactory = valueMapperFactory;
    }

    /**
     * Processes a document according to the given schema definition and gives an Exasol result row. If a non-root table
     * is queried multiple results can be returned.
     *
     * @param document document to map
     * @return stream of exasol rows
     */
    public Stream<List<ValueExpression>> mapRow(final DocumentNode<DocumentVisitorType> document) {
        final DocumentPathExpression pathToNestedTable = this.query.getFromTable().getPathToNestedTable();
        final DocumentPathIteratorFactory<DocumentVisitorType> arrayAllCombinationIterable = new DocumentPathIteratorFactory<>(
                pathToNestedTable, document);
        return arrayAllCombinationIterable.stream().map(iterationState -> mapColumns(document, iterationState));
    }

    private List<ValueExpression> mapColumns(final DocumentNode<DocumentVisitorType> document,
            final PathIterationStateProvider arrayAllIterationState) {
        final List<ValueExpression> resultValues = new ArrayList<>(this.query.getSelectList().size());
        for (final AbstractColumnMappingDefinition resultColumn : this.query.getSelectList()) {
            final AbstractValueMapper<DocumentVisitorType> valueMapper = this.valueMapperFactory
                    .getValueMapperForColumn(resultColumn);
            final ValueExpression result = valueMapper.mapRow(document, arrayAllIterationState);
            resultValues.add(result);
        }
        return resultValues;
    }
}