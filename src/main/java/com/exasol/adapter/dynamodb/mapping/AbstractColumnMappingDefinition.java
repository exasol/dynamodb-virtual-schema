package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.sql.expression.rendering.ValueExpressionRenderer;
import com.exasol.sql.rendering.StringRendererConfig;

public abstract class AbstractColumnMappingDefinition implements ColumnMappingDefinition {
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

    public String getExasolColumnName() {
        return this.exasolColumnName;
    }

    public String getExasolDefaultValueLiteral() {
        final StringRendererConfig stringRendererConfig = StringRendererConfig.createDefault();
        final ValueExpressionRenderer renderer = new ValueExpressionRenderer(stringRendererConfig);
        getExasolDefaultValue().accept(renderer);
        return renderer.render();
    }

    public LookupFailBehaviour getLookupFailBehaviour() {
        return this.lookupFailBehaviour;
    }

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
         * Creates a parameter object for {@link AbstractColumnMappingDefinition(ConstructorParameters)}
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
