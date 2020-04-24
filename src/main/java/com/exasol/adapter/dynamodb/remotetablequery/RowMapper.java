package com.exasol.adapter.dynamodb.remotetablequery;

import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.AbstractValueMapper;
import com.exasol.adapter.dynamodb.mapping.ValueMapperFactory;
import com.exasol.sql.expression.ValueExpression;

/**
 * Maps document data to Exasol rows according to {@link RemoteTableQuery}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class RowMapper<DocumentVisitorType> {
    private final RemoteTableQuery<DocumentVisitorType> remoteTableQuery;
    private final ValueMapperFactory<DocumentVisitorType> valueMapperFactory;

    /**
     * Creates a new {@link RowMapper} for a query described in queryResultTableSchema
     *
     * @param remoteTableQuery   schema of the query result
     * @param valueMapperFactory factory for value mapper corresponding to {@link DocumentVisitorType}
     */
    public RowMapper(final RemoteTableQuery<DocumentVisitorType> remoteTableQuery,
            final ValueMapperFactory<DocumentVisitorType> valueMapperFactory) {
        this.remoteTableQuery = remoteTableQuery;
        this.valueMapperFactory = valueMapperFactory;
    }

    /**
     * Processes a document according to the given schema definition and gives an Exasol result row.
     *
     * @param document document to map
     */
    public List<ValueExpression> mapRow(final DocumentNode<DocumentVisitorType> document) {
        final List<ValueExpression> resultValues = new ArrayList<>(this.remoteTableQuery.getSelectList().size());
        for (final AbstractColumnMappingDefinition resultColumn : this.remoteTableQuery.getSelectList()) {
            final AbstractValueMapper<DocumentVisitorType> valueMapper = this.valueMapperFactory
                    .getValueMapperForColumn(resultColumn);
            final ValueExpression result = valueMapper.mapRow(document);
            resultValues.add(result);
        }
        return resultValues;
    }
}
