package com.exasol.adapter.dynamodb.queryresultschema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.AbstractValueMapper;
import com.exasol.adapter.dynamodb.mapping.ValueMapperFactory;
import com.exasol.sql.expression.ValueExpression;

/**
 * Maps DynamoDB rows to Exasol rows according to {@link QueryResultTableSchema}.
 */
public class RowMapper {
    private final QueryResultTableSchema queryResultTableSchema;

    /**
     * Creates a new {@link RowMapper} for a query described in queryResultTableSchema
     * 
     * @param queryResultTableSchema schema of the query result
     */
    public RowMapper(final QueryResultTableSchema queryResultTableSchema) {
        this.queryResultTableSchema = queryResultTableSchema;
    }

    /**
     * Processes a row according to the given schema definition and gives an Exasol result row.
     *
     * @param dynamodbRow DynamoDB row
     */
    public List<ValueExpression> mapRow(final Map<String, AttributeValue> dynamodbRow) throws AdapterException {
        final List<ValueExpression> resultValues = new ArrayList<>(this.queryResultTableSchema.getColumns().size());
        for (final AbstractColumnMappingDefinition resultColumn : this.queryResultTableSchema.getColumns()) {
            final AbstractValueMapper valueMapper = new ValueMapperFactory().getValueMapperForColumn(resultColumn);
            final ValueExpression result = valueMapper.mapRow(dynamodbRow);
            resultValues.add(result);
        }
        return resultValues;
    }
}
