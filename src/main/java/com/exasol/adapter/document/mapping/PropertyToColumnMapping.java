package com.exasol.adapter.document.mapping;

import com.exasol.adapter.document.documentpath.DocumentPathExpression;

/**
 * This interface defines the mapping from a property in the remote document to an Exasol column.
 */
public interface PropertyToColumnMapping extends ColumnMapping {

    /**
     * Get the path to the property to extract.
     *
     * @return path to the property to extract
     */
    public DocumentPathExpression getPathToSourceProperty();

    /**
     * Get the {@link MappingErrorBehaviour} used in case that the path does not exist in the document.
     *
     * @return {@link MappingErrorBehaviour}
     */
    public MappingErrorBehaviour getLookupFailBehaviour();

    public void accept(PropertyToColumnMappingVisitor visitor);

    @Override
    default void accept(final ColumnMappingVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Builder for {@link PropertyToJsonColumnMapping}.
     */
    public interface Builder {
        /**
         * Set the name of the Exasol column
         *
         * @param exasolColumnName name of the Exasol column
         * @return self
         */
        public Builder exasolColumnName(final String exasolColumnName);

        /**
         * Set the path to the property to extract.
         *
         * @param pathToSourceProperty path to the property to extract.
         * @return self
         */
        public Builder pathToSourceProperty(final DocumentPathExpression pathToSourceProperty);

        /**
         * Set the behaviour for the case, that the defined path does not exist
         *
         * @param lookupFailBehaviour behaviour for the case, that the defined path does not exist
         * @return self
         */
        public Builder lookupFailBehaviour(final MappingErrorBehaviour lookupFailBehaviour);

        /**
         * Build the {@link PropertyToColumnMapping}.
         * 
         * @return built {@link PropertyToColumnMapping}
         */
        public PropertyToColumnMapping build();
    }
}
