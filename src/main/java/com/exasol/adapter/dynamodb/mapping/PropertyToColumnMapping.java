package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

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
     * Get the {@link LookupFailBehaviour} used in case that the path does not exist in the document.
     *
     * @return {@link LookupFailBehaviour}
     */
    public LookupFailBehaviour getLookupFailBehaviour();

    public void accept(PropertyToColumnMappingVisitor visitor);

    @Override
    default void accept(final ColumnMappingVisitor visitor) {
        visitor.visit(this);
    }
}
