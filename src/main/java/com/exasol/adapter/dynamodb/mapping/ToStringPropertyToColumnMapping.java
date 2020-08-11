package com.exasol.adapter.dynamodb.mapping;

import java.util.Objects;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;

/**
 * This class defines a mapping that extracts a string from the remote document and maps it to an Exasol VARCHAR column.
 */
public final class ToStringPropertyToColumnMapping extends AbstractPropertyToColumnMapping {
    private static final long serialVersionUID = 3465558198156097064L;
    private final int varcharColumnSize;
    private final TruncateableMappingErrorBehaviour overflowBehaviour;

    /**
     * Create an instance of {@link ToStringPropertyToColumnMapping}.
     *
     * @param exasolColumnName     Name of the Exasol column
     * @param pathToSourceProperty {@link DocumentPathExpression} path to the property to extract
     * @param lookupFailBehaviour  {@link MappingErrorBehaviour} behaviour for the case, that the defined path does not
     *                             exist
     * @param varcharColumnSize    Size of the Exasol VARCHAR column
     * @param overflowBehaviour    Behaviour if extracted string exceeds {@link #varcharColumnSize}
     */
    public ToStringPropertyToColumnMapping(final String exasolColumnName,
            final DocumentPathExpression pathToSourceProperty, final MappingErrorBehaviour lookupFailBehaviour,
            final int varcharColumnSize, final TruncateableMappingErrorBehaviour overflowBehaviour) {
        super(exasolColumnName, pathToSourceProperty, lookupFailBehaviour);
        this.varcharColumnSize = varcharColumnSize;
        this.overflowBehaviour = overflowBehaviour;
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
     * Get the behaviour if the {@link #varcharColumnSize} is exceeded.
     * 
     * @return {@link TruncateableMappingErrorBehaviour}
     */
    public TruncateableMappingErrorBehaviour getOverflowBehaviour() {
        return this.overflowBehaviour;
    }

    @Override
    public DataType getExasolDataType() {
        return DataType.createVarChar(this.varcharColumnSize, DataType.ExaCharset.UTF8);
    }

    @Override
    public void accept(final PropertyToColumnMappingVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public ColumnMapping withNewExasolName(final String newExasolName) {
        return new ToStringPropertyToColumnMapping(newExasolName, getPathToSourceProperty(), getMappingErrorBehaviour(),
                getVarcharColumnSize(), getOverflowBehaviour());
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ToStringPropertyToColumnMapping)) {
            return false;
        }
        if (!super.equals(other)) {
            return false;
        }
        final ToStringPropertyToColumnMapping that = (ToStringPropertyToColumnMapping) other;
        return this.overflowBehaviour == that.overflowBehaviour && this.varcharColumnSize == that.varcharColumnSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.varcharColumnSize, this.overflowBehaviour);
    }

}
