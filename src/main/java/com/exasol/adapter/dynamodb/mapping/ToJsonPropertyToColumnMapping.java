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
    private static final long serialVersionUID = 8019967800146460950L;
    private final int varcharColumnSize;
    private final OverflowBehaviour overflowBehaviour;

    /**
     * Create an instance of {@link ToJsonPropertyToColumnMapping}.
     * 
     * @param exasolColumnName     Name of the Exasol column
     * @param pathToSourceProperty {@link DocumentPathExpression} path to the property to extract
     * @param lookupFailBehaviour  {@link LookupFailBehaviour} behaviour for the case, that the defined path does not
     * @param varcharColumnSize    Maximum string size of the Exasol column
     * @param overflowBehaviour    Behaviour if the result exceeds the columns size
     */
    public ToJsonPropertyToColumnMapping(final String exasolColumnName,
            final DocumentPathExpression pathToSourceProperty, final LookupFailBehaviour lookupFailBehaviour,
            final int varcharColumnSize, final OverflowBehaviour overflowBehaviour) {
        super(exasolColumnName, pathToSourceProperty, lookupFailBehaviour);
        this.varcharColumnSize = varcharColumnSize;
        this.overflowBehaviour = overflowBehaviour;
    }

    @Override
    public DataType getExasolDataType() {
        return DataType.createVarChar(this.varcharColumnSize, DataType.ExaCharset.UTF8);
    }

    /**
     * Get the maximum string length of the Exasol column
     *
     * @return maximum string length
     */
    public int getVarcharColumnSize() {
        return this.varcharColumnSize;
    }

    @Override
    public ValueExpression getExasolDefaultValue() {
        return StringLiteral.of("");
    }

    @Override
    public boolean isExasolColumnNullable() {
        return true;
    }

    /**
     * Get the {@link OverflowBehaviour} that is used if the result size exceeds the {@link #varcharColumnSize}.
     *
     * @return {@link OverflowBehaviour}
     */
    public OverflowBehaviour getOverflowBehaviour() {
        return this.overflowBehaviour;
    }

    @Override
    public ColumnMapping withNewExasolName(final String newExasolName) {
        return new ToJsonPropertyToColumnMapping(newExasolName, this.getPathToSourceProperty(),
                this.getLookupFailBehaviour(), this.varcharColumnSize, this.overflowBehaviour);
    }

    @Override
    public void accept(final PropertyToColumnMappingVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ToJsonPropertyToColumnMapping)) {
            return false;
        }
        final ToJsonPropertyToColumnMapping that = (ToJsonPropertyToColumnMapping) other;
        return this.varcharColumnSize == that.varcharColumnSize && this.overflowBehaviour.equals(that.overflowBehaviour)
                && super.equals(other);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.getClass().getName(), this.overflowBehaviour,
                this.varcharColumnSize);
    }

    /**
     * Enum with the behaviours if the result of the {@link ToJsonPropertyToColumnMapping} exceeds the defined column
     * size.
     */
    public enum OverflowBehaviour {
        EXCEPTION, NULL
    }
}
