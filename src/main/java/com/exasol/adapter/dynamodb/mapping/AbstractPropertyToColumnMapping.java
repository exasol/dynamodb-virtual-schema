package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This abstract class is the common basis for the {@link PropertyToColumnMapping}s.
 */
public abstract class AbstractPropertyToColumnMapping implements PropertyToColumnMapping {
    private static final long serialVersionUID = -4115453664059509479L;
    private final String exasolColumnName;
    private final DocumentPathExpression pathToSourceProperty;
    private final LookupFailBehaviour lookupFailBehaviour;

    /**
     * Creates an instance of {@link ColumnMapping}.
     *
     * @param parameters parameter object
     */
    AbstractPropertyToColumnMapping(final ConstructorParameters parameters) {
        this.exasolColumnName = parameters.exasolColumnName;
        this.pathToSourceProperty = parameters.pathToSourceProperty;
        this.lookupFailBehaviour = parameters.lookupFailBehaviour;
    }

    @Override
    public String getExasolColumnName() {
        return this.exasolColumnName;
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
     * Parameter object for {@link AbstractPropertyToColumnMapping (ConstructorParameters)}
     */
    public static class ConstructorParameters {
        private final String exasolColumnName;
        private final DocumentPathExpression pathToSourceProperty;
        private final LookupFailBehaviour lookupFailBehaviour;

        /**
         * Creates a parameter object for {@link AbstractPropertyToColumnMapping (ConstructorParameters)}.
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
