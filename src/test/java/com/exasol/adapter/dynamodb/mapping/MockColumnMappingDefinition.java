package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

public class MockColumnMappingDefinition extends AbstractColumnMappingDefinition {
    public MockColumnMappingDefinition(final String destinationName, final DocumentPathExpression resultWalker,
            final LookupFailBehaviour lookupFailBehaviour) {
        super(new ConstructorParameters(destinationName, resultWalker, lookupFailBehaviour));
    }

    @Override
    public DataType getExasolDataType() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public ValueExpression getExasolDefaultValue() {
        return StringLiteral.of("default");
    }

    @Override
    public boolean isExasolColumnNullable() {
        return false;
    }

    @Override
    public void accept(final ColumnMappingDefinitionVisitor visitor) {
        throw new UnsupportedOperationException("not implemented");
    }
}
