package com.exasol.adapter.dynamodb.mapping.tojson;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.AbstractValueMapper;
import com.exasol.adapter.dynamodb.mapping.ValueMapperException;
import com.exasol.dynamodb.AttributeValueToJsonConverter;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

import javax.json.JsonValue;

public class ToJsonValueMapper extends AbstractValueMapper {
    public ToJsonValueMapper(final AbstractColumnMappingDefinition column) {
        super(column);
    }

    @Override
    protected ValueExpression convertValue(final AttributeValue dynamodbProperty) throws ValueMapperException {
        final JsonValue json = AttributeValueToJsonConverter.convert(dynamodbProperty);
        return StringLiteral.of(json.toString());
    }
}
