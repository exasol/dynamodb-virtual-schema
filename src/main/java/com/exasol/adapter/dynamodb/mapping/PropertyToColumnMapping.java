package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This interface defines the mapping from a property in the remote document to an Exasol column.
 */
public interface PropertyToColumnMapping extends ColumnMapping {

    /**
     * Get the {@link LookupFailBehaviour}.
     *
     * @return {@link LookupFailBehaviour}
     */
    public LookupFailBehaviour getLookupFailBehaviour();

    /**
     * Gives the path to the property in the remote document that is mapped by this definition.
     *
     * @return path to property
     */
    public DocumentPathExpression getPathToSourceProperty();

    public void accept(PropertyToColumnMappingVisitor visitor);

    @Override
    default void accept(final ColumnMappingVisitor visitor) {
        visitor.visit(this);
    }
}
