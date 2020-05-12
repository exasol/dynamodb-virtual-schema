package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

public class MockPropertyToColumnMapping extends AbstractPropertyToColumnMapping {
    private static final long serialVersionUID = -4381185567913054550L;

    public MockPropertyToColumnMapping(final String destinationName, final DocumentPathExpression sourcePath,
            final LookupFailBehaviour lookupFailBehaviour) {
        super(destinationName, sourcePath, lookupFailBehaviour);
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
    public void accept(final PropertyToColumnMappingVisitor visitor) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public ColumnMapping copyWithNewExasolName(final String newExasolName) {
        return new MockPropertyToColumnMapping(newExasolName, getPathToSourceProperty(), getLookupFailBehaviour());
    }
}
