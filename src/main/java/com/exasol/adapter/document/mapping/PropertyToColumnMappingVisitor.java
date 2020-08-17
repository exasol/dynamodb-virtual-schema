package com.exasol.adapter.document.mapping;

/**
 * Visitor for {@link PropertyToColumnMapping}.
 */
public interface PropertyToColumnMappingVisitor {
    /**
     * Visits a {@link PropertyToVarcharColumnMapping}.
     * 
     * @param columnDefinition {@link PropertyToVarcharColumnMapping} to visit
     */
    public void visit(PropertyToVarcharColumnMapping columnDefinition);

    /**
     * Visits a {@link PropertyToJsonColumnMapping}.
     * 
     * @param columnDefinition {@link PropertyToJsonColumnMapping} to visit
     */
    public void visit(PropertyToJsonColumnMapping columnDefinition);

    /**
     * Visits a {@link PropertyToDecimalColumnMapping}.
     *
     * @param columnDefinition {@link PropertyToDecimalColumnMapping} to visit
     */
    public void visit(PropertyToDecimalColumnMapping columnDefinition);
}
