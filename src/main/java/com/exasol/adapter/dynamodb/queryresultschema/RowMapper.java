package com.exasol.adapter.dynamodb.queryresultschema;

import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.AbstractValueMapper;
import com.exasol.adapter.dynamodb.mapping.ValueMapperFactory;
import com.exasol.sql.expression.ValueExpression;

/**
 * Maps DynamoDB rows to Exasol rows according to {@link QueryResultTableSchema}.
 */
public class RowMapper<DocumentVisitorType> {
    private final QueryResultTableSchema queryResultTableSchema;
    private final ValueMapperFactory<DocumentVisitorType> valueMapperFactory;

    /**
     * Creates a new {@link RowMapper} for a query described in queryResultTableSchema
     *
     * @param queryResultTableSchema schema of the query result
     * @param valueMapperFactory     factory for value mapper corresponding to {@link DocumentVisitorType}
     */
    public RowMapper(final QueryResultTableSchema queryResultTableSchema,
            final ValueMapperFactory<DocumentVisitorType> valueMapperFactory) {
        this.queryResultTableSchema = queryResultTableSchema;
        this.valueMapperFactory = valueMapperFactory;
    }

    /**
     * Processes a row according to the given schema definition and gives an Exasol result row.
     *
     * @param dynamodbRow DynamoDB row
     */
    public List<ValueExpression> mapRow(final DocumentNode<DocumentVisitorType> dynamodbRow) {
        final List<ValueExpression> resultValues = new ArrayList<>(this.queryResultTableSchema.getColumns().size());
        for (final AbstractColumnMappingDefinition resultColumn : this.queryResultTableSchema.getColumns()) {
            final AbstractValueMapper<DocumentVisitorType> valueMapper = this.valueMapperFactory
                    .getValueMapperForColumn(resultColumn);
            final ValueExpression result = valueMapper.mapRow(dynamodbRow);
            resultValues.add(result);
        }
        return resultValues;
    }
}
