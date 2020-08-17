package com.exasol.adapter.document.mapping;

import java.util.Objects;

import com.exasol.adapter.document.documentpath.DocumentPathExpression;

/**
 * This class is an abstract basis for {@link PropertyToColumnMapping}s.
 */
abstract class AbstractPropertyToColumnMapping extends AbstractColumnMapping implements PropertyToColumnMapping {
    private static final long serialVersionUID = -2781971706810760667L;//
    private final DocumentPathExpression pathToSourceProperty;
    private final MappingErrorBehaviour lookupFailBehaviour;

    /**
     * Create an instance of {@link AbstractPropertyToColumnMapping}.
     *
     * @param exasolColumnName     name of the Exasol column
     * @param pathToSourceProperty {@link DocumentPathExpression} path to the property to extract
     * @param lookupFailBehaviour  {@link MappingErrorBehaviour} behaviour for the case, that the defined path does not
     *                             exist
     */
    protected AbstractPropertyToColumnMapping(final String exasolColumnName,
            final DocumentPathExpression pathToSourceProperty, final MappingErrorBehaviour lookupFailBehaviour) {
        super(exasolColumnName);
        this.pathToSourceProperty = pathToSourceProperty;
        this.lookupFailBehaviour = lookupFailBehaviour;
    }

    /**
     * Get the path to the property to extract.
     *
     * @return path to the property to extract
     */
    public final DocumentPathExpression getPathToSourceProperty() {
        return this.pathToSourceProperty;
    }

    /**
     * Get the {@link MappingErrorBehaviour} used in case that the path does not exist in the document.
     *
     * @return {@link MappingErrorBehaviour}
     */
    public final MappingErrorBehaviour getLookupFailBehaviour() {
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
    public boolean isExasolColumnNullable() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.pathToSourceProperty, this.lookupFailBehaviour);
    }
}
