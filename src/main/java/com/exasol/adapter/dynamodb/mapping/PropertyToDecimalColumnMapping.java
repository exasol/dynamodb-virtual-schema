package com.exasol.adapter.dynamodb.mapping;

import java.util.Objects;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;

/**
 * This class defines a mapping that extracts a decimal number from the remote document and maps it to an Exasol DECIMAL
 * column.
 */
public final class PropertyToDecimalColumnMapping extends AbstractPropertyToColumnMapping {
    private static final long serialVersionUID = -2534835248457080092L;
    private final int decimalPrecision;
    private final int decimalScale;
    private final MappingErrorBehaviour overflowBehaviour;
    private final MappingErrorBehaviour notANumberBehaviour;

    /**
     * Create an instance of {@link PropertyToDecimalColumnMapping}.
     * 
     * @param exasolColumnName     Name of the Exasol column
     * @param pathToSourceProperty {@link DocumentPathExpression} path to the property to extract
     * @param lookupFailBehaviour  {@link MappingErrorBehaviour} behaviour for the case, that the defined path does not
     * @param decimalPrecision     Precision of the Exasol DECIMAL column
     * @param decimalScale         Scale of the Exasol DECIMAL column
     * @param overflowBehaviour    Behaviour to apply in case the value exceeds the size of the DECIMAL column
     * @param notANumberBehaviour  Behaviour to apply in case a value is not a number
     */
    private PropertyToDecimalColumnMapping(final String exasolColumnName,
            final DocumentPathExpression pathToSourceProperty, final MappingErrorBehaviour lookupFailBehaviour,
            final int decimalPrecision, final int decimalScale, final MappingErrorBehaviour overflowBehaviour,
            final MappingErrorBehaviour notANumberBehaviour) {
        super(exasolColumnName, pathToSourceProperty, lookupFailBehaviour);
        this.decimalPrecision = decimalPrecision;
        this.decimalScale = decimalScale;
        this.overflowBehaviour = overflowBehaviour;
        this.notANumberBehaviour = notANumberBehaviour;
    }

    /**
     * Get a builder for {@link PropertyToDecimalColumnMapping}.
     *
     * @return builder for {@link PropertyToDecimalColumnMapping}
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void accept(final PropertyToColumnMappingVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public DataType getExasolDataType() {
        return DataType.createDecimal(this.decimalPrecision, this.decimalScale);
    }

    @Override
    public ColumnMapping withNewExasolName(final String newExasolName) {
        return new PropertyToDecimalColumnMapping(newExasolName, getPathToSourceProperty(),
                getMappingErrorBehaviour(), this.decimalPrecision, this.decimalScale, this.overflowBehaviour,
                this.notANumberBehaviour);
    }

    /**
     * Get the precision of the Exasol DECIMAL column.
     *
     * @return precision of the Exasol DECIMAL column
     */
    public int getDecimalPrecision() {
        return this.decimalPrecision;
    }

    /**
     * Get the scale of the Exasol DECIMAL column.
     *
     * @return scale of the Exasol DECIMAL column
     */
    public int getDecimalScale() {
        return this.decimalScale;
    }

    /**
     * Get the behaviour for input values that exceed the precision of the DECIMAL type.
     *
     * @return {@link MappingErrorBehaviour}
     */
    public MappingErrorBehaviour getOverflowBehaviour() {
        return this.overflowBehaviour;
    }

    /**
     * Get the behaviour that is applied if the value from the document is not a number.
     *
     * @return {@link MappingErrorBehaviour}
     */
    public MappingErrorBehaviour getNotANumberBehaviour() {
        return this.notANumberBehaviour;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PropertyToDecimalColumnMapping)) {
            return false;
        }
        final PropertyToDecimalColumnMapping that = (PropertyToDecimalColumnMapping) other;
        return super.equals(other)//
                && this.decimalPrecision == that.decimalPrecision//
                && this.decimalScale == that.decimalScale//
                && this.overflowBehaviour == that.overflowBehaviour//
                && this.notANumberBehaviour == that.notANumberBehaviour;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.decimalPrecision, this.decimalScale, this.overflowBehaviour,
                this.notANumberBehaviour);
    }

    /**
     * Builder for {@link PropertyToDecimalColumnMapping}
     */
    public static final class Builder implements PropertyToColumnMapping.Builder {
        private String exasolColumnName;
        private DocumentPathExpression pathToSourceProperty;
        private MappingErrorBehaviour lookupFailBehaviour;
        private int decimalPrecision;
        private int decimalScale;
        private MappingErrorBehaviour overflowBehaviour;
        private MappingErrorBehaviour notANumberBehaviour;

        private Builder() {
        }

        @Override
        public Builder exasolColumnName(final String exasolColumnName) {
            this.exasolColumnName = exasolColumnName;
            return this;
        }

        @Override
        public Builder pathToSourceProperty(final DocumentPathExpression pathToSourceProperty) {
            this.pathToSourceProperty = pathToSourceProperty;
            return this;
        }

        @Override
        public Builder lookupFailBehaviour(final MappingErrorBehaviour lookupFailBehaviour) {
            this.lookupFailBehaviour = lookupFailBehaviour;
            return this;
        }

        /**
         * Set the precision of the Exasol DECIMAL column.
         * 
         * @param decimalPrecision precision of the Exasol DECIMAL column
         * @return self
         */
        public Builder decimalPrecision(final int decimalPrecision) {
            this.decimalPrecision = decimalPrecision;
            return this;
        }

        /**
         * Set the scale of the Exasol DECIMAL column.
         * 
         * @param decimalScale scale of the Exasol DECIMAL column
         * @return self
         */
        public Builder decimalScale(final int decimalScale) {
            this.decimalScale = decimalScale;
            return this;
        }

        /**
         * Set the Behaviour to apply in case the value exceeds the size of the DECIMAL column.
         * 
         * @param overflowBehaviour behaviour to apply in case the value exceeds the size of the DECIMAL column
         * @return self
         */
        public Builder overflowBehaviour(final MappingErrorBehaviour overflowBehaviour) {
            this.overflowBehaviour = overflowBehaviour;
            return this;
        }

        /**
         * Set the behaviour to apply in case a value is not a number.
         * 
         * @param notANumberBehaviour behaviour to apply in case a value is not a number
         * @return self
         */
        public Builder notANumberBehaviour(final MappingErrorBehaviour notANumberBehaviour) {
            this.notANumberBehaviour = notANumberBehaviour;
            return this;
        }

        /**
         * Build the {@link PropertyToDecimalColumnMapping}.
         * 
         * @return built {@link PropertyToDecimalColumnMapping}
         */
        public PropertyToDecimalColumnMapping build() {
            return new PropertyToDecimalColumnMapping(this.exasolColumnName, this.pathToSourceProperty,
                    this.lookupFailBehaviour, this.decimalPrecision, this.decimalScale, this.overflowBehaviour,
                    this.notANumberBehaviour);
        }
    }
}
