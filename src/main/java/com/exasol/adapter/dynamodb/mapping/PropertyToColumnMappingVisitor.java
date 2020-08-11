package com.exasol.adapter.dynamodb.mapping;

/**
 * Visitor for {@link PropertyToColumnMapping}.
 */
public interface PropertyToColumnMappingVisitor {
    /**
     * Visits a {@link ToStringPropertyToColumnMapping}.
     * 
     * @param columnDefinition {@link ToStringPropertyToColumnMapping} to visit
     */
    public void visit(ToStringPropertyToColumnMapping columnDefinition);

    /**
     * Visits a {@link ToJsonPropertyToColumnMapping}.
     * 
     * @param columnDefinition {@link ToJsonPropertyToColumnMapping} to visit
     */
    public void visit(ToJsonPropertyToColumnMapping columnDefinition);

    /**
     * Visits a {@link ToDecimalPropertyToColumnMapping}.
     *
     * @param columnDefinition {@link ToDecimalPropertyToColumnMapping} to visit
     */
    public void visit(ToDecimalPropertyToColumnMapping columnDefinition);
}
