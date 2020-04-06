package com.exasol.adapter.dynamodb.mapping.tojsonmapping;

import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.ColumnMappingDefinitionVisitor;
import com.exasol.adapter.metadata.DataType;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * Maps a property of a DynamoDB table and all it's descendants to a JSON string
 */
public class ToJsonColumnMappingDefinition extends AbstractColumnMappingDefinition {
    private static final long serialVersionUID = 7687302490848045236L;

    /**
     * Creates an instance of {@link ToJsonColumnMappingDefinition}.
     * 
     * @param parameters Parameter object for {@link AbstractColumnMappingDefinition}
     */
    public ToJsonColumnMappingDefinition(final ConstructorParameters parameters) {
        super(parameters);
    }

    @Override
    public DataType getDestinationDataType() {
        return DataType.createVarChar(10000, DataType.ExaCharset.UTF8);
    }

    @Override
    public ValueExpression getDestinationDefaultValue() {
        return StringLiteral.of("");
    }

    @Override
    public boolean isDestinationNullable() {
        return true;
    }

    @Override
    public void accept(final ColumnMappingDefinitionVisitor visitor) {
        visitor.visit(this);
    }
}
