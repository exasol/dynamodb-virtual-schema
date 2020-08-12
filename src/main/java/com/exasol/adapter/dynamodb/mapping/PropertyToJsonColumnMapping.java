package com.exasol.adapter.dynamodb.mapping;

import java.util.Objects;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;

/**
 * Maps a property of a DynamoDB table and all its descendants to a JSON string.
 */
public final class PropertyToJsonColumnMapping extends AbstractPropertyToColumnMapping {
    private static final long serialVersionUID = -6383134783719798072L;//
    private final int varcharColumnSize;
    private final MappingErrorBehaviour overflowBehaviour;

    /**
     * Create an instance of {@link PropertyToJsonColumnMapping}.
     * 
     * @param exasolColumnName     Name of the Exasol column
     * @param pathToSourceProperty {@link DocumentPathExpression} path to the property to extract
     * @param lookupFailBehaviour  {@link MappingErrorBehaviour} behaviour for the case, that the defined path does not
     *                             exist
     * @param varcharColumnSize    Size of the Exasol VARCHAR column
     * @param overflowBehaviour    Behaviour if the result exceeds the columns size
     */
    public PropertyToJsonColumnMapping(final String exasolColumnName, final DocumentPathExpression pathToSourceProperty,
            final MappingErrorBehaviour lookupFailBehaviour, final int varcharColumnSize,
            final MappingErrorBehaviour overflowBehaviour) {
        super(exasolColumnName, pathToSourceProperty, lookupFailBehaviour);
        this.varcharColumnSize = varcharColumnSize;
        this.overflowBehaviour = overflowBehaviour;
    }

    @Override
    public DataType getExasolDataType() {
        return DataType.createVarChar(this.varcharColumnSize, DataType.ExaCharset.UTF8);
    }

    /**
     * Get the size of the Exasol VARCHAR column.
     *
     * @return size of Exasol VARCHAR column
     */
    public int getVarcharColumnSize() {
        return this.varcharColumnSize;
    }

    /**
     * Get the {@link MappingErrorBehaviour} that is used if the result size exceeds the {@link #varcharColumnSize}.
     *
     * @return {@link MappingErrorBehaviour}
     */
    public MappingErrorBehaviour getOverflowBehaviour() {
        return this.overflowBehaviour;
    }

    @Override
    public ColumnMapping withNewExasolName(final String newExasolName) {
        return new PropertyToJsonColumnMapping(newExasolName, this.getPathToSourceProperty(),
                this.getMappingErrorBehaviour(), this.varcharColumnSize, this.overflowBehaviour);
    }

    @Override
    public void accept(final PropertyToColumnMappingVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PropertyToJsonColumnMapping)) {
            return false;
        }
        final PropertyToJsonColumnMapping that = (PropertyToJsonColumnMapping) other;
        return this.varcharColumnSize == that.varcharColumnSize && this.overflowBehaviour.equals(that.overflowBehaviour)
                && super.equals(other);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.getClass().getName(), this.overflowBehaviour,
                this.varcharColumnSize);
    }
}