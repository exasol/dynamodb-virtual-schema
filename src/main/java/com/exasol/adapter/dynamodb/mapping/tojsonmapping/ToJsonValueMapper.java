package com.exasol.adapter.dynamodb.mapping.tojsonmapping;

import javax.json.JsonValue;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.AbstractValueMapper;
import com.exasol.adapter.dynamodb.mapping.ValueMapperException;
import com.exasol.dynamodb.AttributeValueToJsonConverter;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * ValueMapper for {@link ToJsonColumnMappingDefinition}
 */
public class ToJsonValueMapper extends AbstractValueMapper {

    /**
     * Creates an instance of {@link ToJsonColumnMappingDefinition}
     * 
     * @param column {@link ToJsonColumnMappingDefinition}
     */
    public ToJsonValueMapper(final AbstractColumnMappingDefinition column) {
        super(column);
    }

    @Override
    protected ValueExpression mapValue(final AttributeValue dynamodbProperty) throws ValueMapperException {
        final JsonValue json = AttributeValueToJsonConverter.convert(dynamodbProperty);
        return StringLiteral.of(json.toString());
    }
}
