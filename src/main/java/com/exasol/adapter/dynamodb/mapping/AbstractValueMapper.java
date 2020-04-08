package com.exasol.adapter.dynamodb.mapping;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
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
     * @param column ColumnMappingDefinition defining the mapping
     */
    public AbstractValueMapper(final AbstractColumnMappingDefinition column) {
        this.column = column;
    }

    /**
     * Extracts {@link #column}s value from DynamoDB's result row.
     *
     * @param dynamodbRow to extract the value from
     * @return {@link ValueExpression}
     * @throws DynamodbResultWalkerException if specified property can't be extracted and
     *                                       {@link AbstractColumnMappingDefinition.LookupFailBehaviour} exception
     * @throws ValueMapperException          if specified property can't be mapped and
     *                                       {@link AbstractColumnMappingDefinition.LookupFailBehaviour} exception
     */
    public ValueExpression mapRow(final Map<String, AttributeValue> dynamodbRow)
            throws DynamodbResultWalkerException, ValueMapperException {
        try {
            final AttributeValue dynamodbProperty = this.column.getPathToSourceProperty().walk(dynamodbRow);
            return this.mapValue(dynamodbProperty);
        } catch (final DynamodbResultWalkerException | LookupValueMapperException exception) {
            if (this.column
                    .getLookupFailBehaviour() == AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE) {
                return this.column.getExasolDefaultValue();
            }
            else {
                throw exception;
            }
        }
    }

    /**
     * Converts the DynamoDB property into an Exasol {@link ValueExpression}.
     *
     * @param dynamodbProperty the DynamoDB property to be converted
     * @return the conversion result
     * @throws ValueMapperException if the value can't be mapped
     */
    protected abstract ValueExpression mapValue(AttributeValue dynamodbProperty) throws ValueMapperException;
}
