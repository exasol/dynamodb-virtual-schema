package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.sql.expression.rendering.ValueExpressionRenderer;
import com.exasol.sql.rendering.StringRendererConfig;

/**
 * This abstract class is the common basis for the {@link ColumnMappingDefinition}s.
 * <p>
 * Objects of this class get serialized into the column adapter notes. They are created using a
 * {@link MappingDefinitionFactory}. Storing the mapping definition is necessary as mapping definition files in BucketFS
 * could be changed, but the mapping must not be changed until a {@code REFRESH} statement is called.
 * </p>
 */
public abstract class AbstractColumnMappingDefinition implements ColumnMappingDefinition {
    private static final long serialVersionUID = -4115453664059509479L;
    private final String exasolColumnName;
    private final DocumentPathExpression pathToSourceProperty;
    private final LookupFailBehaviour lookupFailBehaviour;

    /**
     * Creates an instance of {@link ColumnMappingDefinition}.
     *
     * @param parameters parameter object
     */
    AbstractColumnMappingDefinition(final ConstructorParameters parameters) {
        this.exasolColumnName = parameters.exasolColumnName;
        this.pathToSourceProperty = parameters.pathToSourceProperty;
        this.lookupFailBehaviour = parameters.lookupFailBehaviour;
    }

    @Override
    public String getExasolColumnName() {
        return this.exasolColumnName;
    }

    @Override
    public String getExasolDefaultValueLiteral() {
        final StringRendererConfig stringRendererConfig = StringRendererConfig.createDefault();
        final ValueExpressionRenderer renderer = new ValueExpressionRenderer(stringRendererConfig);
        getExasolDefaultValue().accept(renderer);
        return renderer.render();
    }

    @Override
    public LookupFailBehaviour getLookupFailBehaviour() {
        return this.lookupFailBehaviour;
    }

    @Override
    public DocumentPathExpression getPathToSourceProperty() {
        return this.pathToSourceProperty;
    }

    /**
     * Parameter object for {@link AbstractColumnMappingDefinition(ConstructorParameters)}
     */
    public static class ConstructorParameters {
        private final String exasolColumnName;
        private final DocumentPathExpression pathToSourceProperty;
        private final LookupFailBehaviour lookupFailBehaviour;

        /**
         * Creates a parameter object for {@link AbstractColumnMappingDefinition(ConstructorParameters)}.
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
