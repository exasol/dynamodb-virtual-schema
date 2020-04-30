package com.exasol.adapter.dynamodb.mapping;

import java.io.Serializable;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;
import com.exasol.sql.expression.ValueExpression;
import com.exasol.sql.expression.rendering.ValueExpressionRenderer;
import com.exasol.sql.rendering.StringRendererConfig;

/**
 * Definition of a column mapping from DynamoDB table to Exasol Virtual Schema.
 * <p>
 * Each instance of this class represents one column in the Exasol table. Objects of this class get serialized into the
 * column adapter notes. They are created using a {@link MappingDefinitionFactory}. Storing the Mapping definition is
 * necessary as mapping definition files in BucketFS could be changed, but the mapping must not be changed until a
 * {@code REFRESH} statement is called.
 * </p>
 */
public abstract class AbstractColumnMappingDefinition implements Serializable {
    private static final long serialVersionUID = 48342992735371252L;
    private final String exasolColumnName;
    private final DocumentPathExpression pathToSourceProperty;
    private final LookupFailBehaviour lookupFailBehaviour;

    /**
     * Creates an instance of {@link AbstractColumnMappingDefinition}.
     * 
     * @param parameters parameter object
     */
    AbstractColumnMappingDefinition(final ConstructorParameters parameters) {
        this.exasolColumnName = parameters.exasolColumnName;
        this.pathToSourceProperty = parameters.pathToSourceProperty;
        this.lookupFailBehaviour = parameters.lookupFailBehaviour;
    }

    /**
     * Get the name of the column in the Exasol table.
     *
     * @return name of the column
     */
    public String getExasolColumnName() {
        return this.exasolColumnName;
    }

    /**
     * Get the Exasol data type.
     *
     * @return Exasol data type
     */
    public abstract DataType getExasolDataType();

    /**
     * Get the default value of this column.
     *
     * @return {@link ValueExpression} holding default value
     */
    public abstract ValueExpression getExasolDefaultValue();

    /**
     * Get the string representation of the exasol column default value literal.
     * 
     * @return default value string
     */
    public String getExasolDefaultValueLiteral() {
        final StringRendererConfig stringRendererConfig = StringRendererConfig.createDefault();
        final ValueExpressionRenderer renderer = new ValueExpressionRenderer(stringRendererConfig);
        getExasolDefaultValue().accept(renderer);
        return renderer.render();
    }

    /**
     * Describes if Exasol column is nullable.
     *
     * @return {@code <true>} if Exasol column is nullable
     */
    public abstract boolean isExasolColumnNullable();

    /**
     * Get the {@link LookupFailBehaviour}
     *
     * @return {@link LookupFailBehaviour}
     */
    public LookupFailBehaviour getLookupFailBehaviour() {
        return this.lookupFailBehaviour;
    }

    public DocumentPathExpression getPathToSourceProperty() {
        return this.pathToSourceProperty;
    }

    public abstract void accept(ColumnMappingDefinitionVisitor visitor);

    /**
     * This enum describes behaviour of the mapping definition when the requested property is not set in a given
     * DynamoDB row.
     */
    public enum LookupFailBehaviour {
        /**
         * Break the execution of the query .
         */
        EXCEPTION,
        /**
         * The column specific default value is returned.
         */
        DEFAULT_VALUE
    }

    /**
     * Parameter object for
     * {@link AbstractColumnMappingDefinition#AbstractColumnMappingDefinition(ConstructorParameters)}
     */
    public static class ConstructorParameters {
        private final String exasolColumnName;
        private final DocumentPathExpression pathToSourceProperty;
        private final LookupFailBehaviour lookupFailBehaviour;

        /**
         * Creates a parameter object for
         * {@link AbstractColumnMappingDefinition#AbstractColumnMappingDefinition(ConstructorParameters)}
         *
         * @param exasolColumnName     name of the Exasol column
         * @param pathToSourceProperty {@link DocumentPathExpression} representing the path to the source DynamoDB
         *                             property
         * @param lookupFailBehaviour  {@link LookupFailBehaviour} if the defined path does not exist
         */
        public ConstructorParameters(final String exasolColumnName, final DocumentPathExpression pathToSourceProperty,
                final LookupFailBehaviour lookupFailBehaviour) {
            this.exasolColumnName = exasolColumnName;
            this.pathToSourceProperty = pathToSourceProperty;
            this.lookupFailBehaviour = lookupFailBehaviour;
        }
    }
}
