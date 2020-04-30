package com.exasol.adapter.dynamodb.mapping;

import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;
import com.exasol.sql.expression.ValueExpression;

/**
 * Maps document data to Exasol rows according to {@link SchemaMappingQuery}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class RowMapper<DocumentVisitorType> {
    private final SchemaMappingQuery query;
    private final ValueMapperFactory<DocumentVisitorType> valueMapperFactory;

    /**
     * Creates a new {@link RowMapper} for a query described in queryResultTableSchema
     *
     * @param query              schema of the query result
     * @param valueMapperFactory factory for value mapper corresponding to {@link DocumentVisitorType}
     */
    public RowMapper(final RemoteTableQuery<DocumentVisitorType> query,
            final ValueMapperFactory<DocumentVisitorType> valueMapperFactory) {
        this.query = query;
        this.valueMapperFactory = valueMapperFactory;
    }

    /**
     * Processes a document according to the given schema definition and gives an Exasol result row.
     *
     * @param document document to map
     */
    public List<ValueExpression> mapRow(final DocumentNode<DocumentVisitorType> document) {
        final List<ValueExpression> resultValues = new ArrayList<>(this.query.getSelectList().size());
        for (final AbstractColumnMappingDefinition resultColumn : this.query.getSelectList()) {
            final AbstractValueMapper<DocumentVisitorType> valueMapper = this.valueMapperFactory
                    .getValueMapperForColumn(resultColumn);
            final ValueExpression result = valueMapper.mapRow(document);
            resultValues.add(result);
        }
        return resultValues;
    }
}
