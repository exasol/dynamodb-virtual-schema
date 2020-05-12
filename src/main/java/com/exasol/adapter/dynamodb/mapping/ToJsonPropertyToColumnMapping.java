package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.metadata.DataType;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * Maps a property of a DynamoDB table and all it's descendants to a JSON string
 */
public class ToJsonPropertyToColumnMapping extends AbstractPropertyToColumnMapping {
    private static final long serialVersionUID = 7687302490848045236L;

    /**
     * Creates an instance of {@link ToJsonPropertyToColumnMapping}.
     * 
     * @param parameters Parameter object for {@link ColumnMapping}
     */
    public ToJsonPropertyToColumnMapping(final ConstructorParameters parameters) {
        super(parameters);
    }

    @Override
    public DataType getExasolDataType() {
        return DataType.createVarChar(10000, DataType.ExaCharset.UTF8);
    }

    @Override
    public ValueExpression getExasolDefaultValue() {
        return StringLiteral.of("");
    }

    @Override
    public boolean isExasolColumnNullable() {
        return true;
    }

    @Override
    public void accept(final PropertyToColumnMappingVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public ColumnMapping copyWithNewExasolName(final String newExasolName) {
        return new ToJsonPropertyToColumnMapping(
                new ConstructorParameters(newExasolName, getPathToSourceProperty(), getLookupFailBehaviour()));
    }
}
