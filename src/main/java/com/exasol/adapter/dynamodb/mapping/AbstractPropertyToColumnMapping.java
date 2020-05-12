package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class is an abstract basis for {@link PropertyToColumnMapping}s.
 */
abstract class AbstractPropertyToColumnMapping extends AbstractColumnMapping implements PropertyToColumnMapping {
    private static final long serialVersionUID = -1909538874201933178L;
    private final DocumentPathExpression pathToSourceProperty;
    private final LookupFailBehaviour lookupFailBehaviour;

    /**
     * Creates an instance of {@link AbstractPropertyToColumnMapping}.
     *
     * @param exasolColumnName     Name of the Exasol column
     * @param pathToSourceProperty {@link DocumentPathExpression} path to the property to extract
     * @param lookupFailBehaviour  {@link LookupFailBehaviour} behaviour for the case, that the defined path does not
     *                             exist
     */
    protected AbstractPropertyToColumnMapping(final String exasolColumnName,
            final DocumentPathExpression pathToSourceProperty, final LookupFailBehaviour lookupFailBehaviour) {
        super(exasolColumnName);
        this.pathToSourceProperty = pathToSourceProperty;
        this.lookupFailBehaviour = lookupFailBehaviour;
    }

    /**
     * Gives the path to the property to extract.
     *
     * @return path to the property to extract
     */
    public final DocumentPathExpression getPathToSourceProperty() {
        return this.pathToSourceProperty;
    }

    /**
     * Gives the {@link LookupFailBehaviour} used in case that the path does not exist in the document.
     *
     * @return {@link LookupFailBehaviour}
     */
    public final LookupFailBehaviour getLookupFailBehaviour() {
        return this.lookupFailBehaviour;
    }
}
