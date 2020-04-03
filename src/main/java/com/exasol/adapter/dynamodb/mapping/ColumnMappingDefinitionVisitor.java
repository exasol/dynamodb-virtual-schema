package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tostringmapping.ToStringColumnMappingDefinition;

/**
 * Visitor for {@link AbstractColumnMappingDefinition}.
 */
public interface ColumnMappingDefinitionVisitor {
	/**
	 * Visits an {@link ToStringColumnMappingDefinition}.
	 * 
	 * @param columnDefinition
	 *            {@link ToStringColumnMappingDefinition} to visit
	 */
	public void visit(ToStringColumnMappingDefinition columnDefinition);

	/**
	 * Visits an {@link ToJsonColumnMappingDefinition}.
	 * 
	 * @param columnDefinition
	 *            {@link ToJsonColumnMappingDefinition} to visit
	 */
	public void visit(ToJsonColumnMappingDefinition columnDefinition);
}
