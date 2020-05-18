package com.exasol.adapter.dynamodb.mapping;

import java.util.Objects;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class is an abstract basis for {@link PropertyToColumnMapping}s.
 */
abstract class AbstractPropertyToColumnMapping extends AbstractColumnMapping implements PropertyToColumnMapping {
    private static final long serialVersionUID = 8355270641073197862L;
    private final DocumentPathExpression pathToSourceProperty;
    private final LookupFailBehaviour lookupFailBehaviour;

    /**
     * Creates an instance of {@link AbstractPropertyToColumnMapping}.
     *
     * @param exasolColumnName     name of the Exasol column
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

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PropertyToColumnMapping)) {
            return false;
        }
        if (!super.equals(other)) {
            return false;
        }
        final PropertyToColumnMapping that = (PropertyToColumnMapping) other;
        if (this.lookupFailBehaviour != that.getLookupFailBehaviour()) {
            return false;
        }
        return this.pathToSourceProperty.equals(that.getPathToSourceProperty());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.pathToSourceProperty, this.lookupFailBehaviour);
    }
}
