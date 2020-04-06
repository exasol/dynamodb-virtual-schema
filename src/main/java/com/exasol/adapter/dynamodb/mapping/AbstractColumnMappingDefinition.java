package com.exasol.adapter.dynamodb.mapping;

import java.io.Serializable;

import com.exasol.adapter.metadata.DataType;
import com.exasol.dynamodb.resultwalker.AbstractDynamodbResultWalker;
import com.exasol.sql.expression.ValueExpression;
import com.exasol.sql.expression.rendering.ValueExpressionRenderer;
import com.exasol.sql.rendering.StringRendererConfig;

/**
 * Definition of a column mapping from DynamoDB table to Exasol Virtual Schema.
 * <p>
 * Each instance of this class represents one column in the Exasol table. Objects of this class get serialized into the
 * column adapter notes. They are created using a {@link MappingDefinitionFactory}. Storing the Mapping definition is
 * necessary as mapping definition files in bucketfs could change but the mapping must not change until {@code REFRESH}
 * is called.
 * </p>
 */
public abstract class AbstractColumnMappingDefinition implements Serializable {
    private static final long serialVersionUID = 48342992735371252L;
    private final String destinationName;
    private final AbstractDynamodbResultWalker pathToSourceProperty;
    private final LookupFailBehaviour lookupFailBehaviour;

    /**
     * Creates an instance of {@link AbstractColumnMappingDefinition}.
     * 
     * @param destinationName     name of the Exasol column
     * @param pathToSourceProperty        {@link AbstractDynamodbResultWalker} representing the path to the source DynamoDB
     *                            property
     * @param lookupFailBehaviour {@link LookupFailBehaviour} if the defined path does not exist
     */
    public AbstractColumnMappingDefinition(final String destinationName,
                                           final AbstractDynamodbResultWalker pathToSourceProperty, final LookupFailBehaviour lookupFailBehaviour) {
        this.destinationName = destinationName;
        this.pathToSourceProperty = pathToSourceProperty;
        this.lookupFailBehaviour = lookupFailBehaviour;
    }

    /**
     * Get the name of the column in the Exasol table.
     * 
     * @return name of the column
     */
    public String getDestinationName() {
        return this.destinationName;
    }

    /**
     * Get the Exasol data type.
     * 
     * @return Exasol data type
     */
    public abstract DataType getDestinationDataType();

    /**
     * Get the default value of this column.
     * 
     * @return {@link ValueExpression} holding default value
     */
    public abstract ValueExpression getDestinationDefaultValue();

    public String getDestinationDefaultValueLiteral() {
        final StringRendererConfig stringRendererConfig = StringRendererConfig.createDefault();
        final ValueExpressionRenderer renderer = new ValueExpressionRenderer(stringRendererConfig);
        this.getDestinationDefaultValue().accept(renderer);
        return renderer.render();
    }

    /**
     * Check if Exasol column is nullable.
     * 
     * @return {@code <true>} if Exasol column is nullable
     */
    public abstract boolean isDestinationNullable();

    /**
     * Get the {@link LookupFailBehaviour}
     * 
     * @return {@link LookupFailBehaviour}
     */
    public LookupFailBehaviour getLookupFailBehaviour() {
        return this.lookupFailBehaviour;
    }

    AbstractDynamodbResultWalker getPathToSourceProperty() {
        return this.pathToSourceProperty;
    }

    public abstract void accept(ColumnMappingDefinitionVisitor visitor);

    /**
     * Behaviour if the requested property is not set in a given DynamoDB row.
     */
    public enum LookupFailBehaviour {
        /**
         * Break the whole query.
         */
        EXCEPTION,
        /**
         * The column specific default value is returned.
         */
        DEFAULT_VALUE
    }
}
