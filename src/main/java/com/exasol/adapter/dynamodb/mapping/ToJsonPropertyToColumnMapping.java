package com.exasol.adapter.dynamodb.mapping;

import java.util.Objects;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * Maps a property of a DynamoDB table and all its descendants to a JSON string.
 */
public final class ToJsonPropertyToColumnMapping extends AbstractPropertyToColumnMapping {
    private static final long serialVersionUID = -8905031243395926232L;

    /**
     * Create an instance of {@link ToJsonPropertyToColumnMapping}.
     *
     * @param exasolColumnName     Name of the Exasol column
     * @param pathToSourceProperty {@link DocumentPathExpression} path to the property to extract
     * @param lookupFailBehaviour  {@link LookupFailBehaviour} behaviour for the case, that the defined path does not
     */
    public ToJsonPropertyToColumnMapping(final String exasolColumnName,
            final DocumentPathExpression pathToSourceProperty, final LookupFailBehaviour lookupFailBehaviour) {
        super(exasolColumnName, pathToSourceProperty, lookupFailBehaviour);
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
    public ColumnMapping withNewExasolName(final String newExasolName) {
        return new ToJsonPropertyToColumnMapping(newExasolName, this.getPathToSourceProperty(),
                this.getLookupFailBehaviour());
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ToJsonPropertyToColumnMapping)) {
            return false;
        }
        return super.equals(other);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.getClass().getName());
    }
}
