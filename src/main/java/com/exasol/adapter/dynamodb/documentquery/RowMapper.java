package com.exasol.adapter.dynamodb.documentquery;

import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.AbstractValueMapper;
import com.exasol.adapter.dynamodb.mapping.ValueMapperFactory;
import com.exasol.sql.expression.ValueExpression;

/**
 * Maps document data to Exasol rows according to {@link DocumentQuery}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class RowMapper<DocumentVisitorType> {
    private final DocumentQuery<DocumentVisitorType> documentQuery;
    private final ValueMapperFactory<DocumentVisitorType> valueMapperFactory;

    /**
     * Creates a new {@link RowMapper} for a query described in queryResultTableSchema
     *
     * @param documentQuery      schema of the query result
     * @param valueMapperFactory factory for value mapper corresponding to {@link DocumentVisitorType}
     */
    public RowMapper(final DocumentQuery<DocumentVisitorType> documentQuery,
            final ValueMapperFactory<DocumentVisitorType> valueMapperFactory) {
        this.documentQuery = documentQuery;
        this.valueMapperFactory = valueMapperFactory;
    }

    /**
     * Processes a document according to the given schema definition and gives an Exasol result row.
     *
     * @param document document to map
     */
    public List<ValueExpression> mapRow(final DocumentNode<DocumentVisitorType> document) {
        final List<ValueExpression> resultValues = new ArrayList<>(this.documentQuery.getSelectList().size());
        for (final AbstractColumnMappingDefinition resultColumn : this.documentQuery.getSelectList()) {
            final AbstractValueMapper<DocumentVisitorType> valueMapper = this.valueMapperFactory
                    .getValueMapperForColumn(resultColumn);
            final ValueExpression result = valueMapper.mapRow(document);
            resultValues.add(result);
        }
        return resultValues;
    }
}
