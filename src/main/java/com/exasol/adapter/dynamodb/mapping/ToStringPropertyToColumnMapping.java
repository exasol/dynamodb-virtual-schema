package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * This class defines a mapping that extracts a string from the remote document and maps it to an Exasol VARCHAR column.
 */
public class ToStringPropertyToColumnMapping extends AbstractPropertyToColumnMapping {
    private static final long serialVersionUID = -6772281079326146978L;
    private final int exasolStringSize;
    private final OverflowBehaviour overflowBehaviour;

    /**
     * Creates an instance of {@link ToStringPropertyToColumnMapping}.
     *
     * @param exasolColumnName     Name of the Exasol column
     * @param pathToSourceProperty {@link DocumentPathExpression} path to the property to extract
     * @param lookupFailBehaviour  {@link LookupFailBehaviour} behaviour for the case, that the defined path does not
     *                             exist
     * @param exasolStringSize     Length of the Exasol VARCHAR
     * @param overflowBehaviour    Behaviour if extracted string exceeds {@link #exasolStringSize}
     */
    public ToStringPropertyToColumnMapping(final String exasolColumnName,
            final DocumentPathExpression pathToSourceProperty, final LookupFailBehaviour lookupFailBehaviour,
            final int exasolStringSize, final OverflowBehaviour overflowBehaviour) {
        super(exasolColumnName, pathToSourceProperty, lookupFailBehaviour);
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
    public void accept(final PropertyToColumnMappingVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public ColumnMapping withNewExasolName(final String newExasolName) {
        return new ToStringPropertyToColumnMapping(newExasolName, getPathToSourceProperty(), getLookupFailBehaviour(),
                getExasolStringSize(), getOverflowBehaviour());
    }

    /**
     * Specifies the behaviour of {@link ToStringPropertyToColumnValueExtractor} if the string from DynamoDB is longer
     * than {@link #exasolStringSize}.
     */
    public enum OverflowBehaviour {
        /**
         * truncate the string to the configured length.
         */
        TRUNCATE,

        /**
         * throw an {@link ToStringPropertyToColumnValueExtractor.OverflowException}.
         */
        EXCEPTION
    }
}
