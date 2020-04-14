package com.exasol.adapter.dynamodb.mapping.tostringmapping;

import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.ColumnMappingDefinitionVisitor;
import com.exasol.adapter.metadata.DataType;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * Extracts a string from a DynamoDB table and maps to a Exasol VarChar column
 */
public class ToStringColumnMappingDefinition extends AbstractColumnMappingDefinition {
    private static final long serialVersionUID = -6772281079326146978L;
    private final int exasolStringSize;
    private final OverflowBehaviour overflowBehaviour;

    /**
     * Creates an instance of {@link ToStringColumnMappingDefinition}.
     * 
     * @param parameters        Parameter object for {{@link AbstractColumnMappingDefinition}}
     * @param exasolStringSize  Length of the Exasol VARCHAR
     * @param overflowBehaviour Behaviour if extracted string exceeds {@link #exasolStringSize}
     */
    public ToStringColumnMappingDefinition(final ConstructorParameters parameters, final int exasolStringSize,
            final OverflowBehaviour overflowBehaviour) {
        super(parameters);
        this.exasolStringSize = exasolStringSize;
        this.overflowBehaviour = overflowBehaviour;
    }

    /**
     * Get the maximum Exasol VARCHAR size.
     * 
     * @return maximum size of Exasol VARCHAR
     */
    public int getExasolStringSize() {
        return this.exasolStringSize;
    }

    /**
     * Get the behaviour if the {@link #exasolStringSize} is exceeded.
     * 
     * @return {@link OverflowBehaviour}
     */
    public OverflowBehaviour getOverflowBehaviour() {
        return this.overflowBehaviour;
    }

    @Override
    public DataType getExasolDataType() {
        return DataType.createVarChar(this.exasolStringSize, DataType.ExaCharset.UTF8);
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
    public void accept(final ColumnMappingDefinitionVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Specifies the behaviour of {@link ToStringValueMapper} if the string from DynamoDB is longer than
     * {@link #exasolStringSize}.
     */
    public enum OverflowBehaviour {
        /**
         * truncate the string to the configured length.
         */
        TRUNCATE,

        /**
         * throw an {@link ToStringValueMapper.OverflowException}.
         */
        EXCEPTION
    }
}
