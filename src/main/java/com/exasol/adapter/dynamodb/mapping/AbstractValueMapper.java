package com.exasol.adapter.dynamodb.mapping;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.dynamodb.resultwalker.DynamodbResultWalkerException;
import com.exasol.sql.expression.ValueExpression;

/**
 * Abstract class for extracting a value specified in an ColumnMappingDefinition from a DynamoDB row.
 */
public abstract class AbstractValueMapper {
    private final AbstractColumnMappingDefinition column;

    /**
     * Creates an instance of {@link AbstractValueMapper} for extracting a value specified parameter column from a
     * DynamoDB row.
     * 
     * @param column
     */
    public AbstractValueMapper(final AbstractColumnMappingDefinition column) {
        this.column = column;
    }

    /**
     * Extracts {@link #column}s value from DynamoDB's result row.
     *
     * @param dynamodbRow
     * @return {@link ValueExpression}
     * @throws AdapterException
     */
    public ValueExpression mapRow(final Map<String, AttributeValue> dynamodbRow)
            throws DynamodbResultWalkerException, ValueMapperException {
        try {
            final AttributeValue dynamodbProperty = this.column.getPathToSourceProperty().walk(dynamodbRow);
            return this.mapValue(dynamodbProperty);
        } catch (final DynamodbResultWalkerException | LookupValueMapperException exception) {
            if (this.column
                    .getLookupFailBehaviour() == AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE) {
                return this.column.getDestinationDefaultValue();
            }
            throw exception;
        }
    }

    /**
     * Converts the DynamoDB property into an Exasol cell value.
     *
     * @param dynamodbProperty the DynamoDB property to be converted
     * @return the conversion result
     * @throws ValueMapperException
     */
    protected abstract ValueExpression mapValue(AttributeValue dynamodbProperty) throws ValueMapperException;
}
