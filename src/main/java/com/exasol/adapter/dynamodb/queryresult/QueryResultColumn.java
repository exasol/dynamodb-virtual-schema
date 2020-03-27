package com.exasol.adapter.dynamodb.queryresult;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.cellvalue.ExasolCellValue;

/**
 * This class represents one column in a query result. It is used for building
 * the actual result using calls to {@link #convertRow(Map)}.
 *
 * Right now this class seems unnecessary as
 * {@link AbstractColumnMappingDefinition#convertRow(Map)} could be used directly. It
 * will be needed in the future for representing constant values from the
 * {@code SELECT}.
 */
public class QueryResultColumn {
	private final AbstractColumnMappingDefinition columnMapping;

	/**
	 * Constructor
	 * 
	 * @param columnMapping
	 *            the {@link AbstractColumnMappingDefinition} for this column
	 */
	public QueryResultColumn(final AbstractColumnMappingDefinition columnMapping) {
		this.columnMapping = columnMapping;
	}

	/**
	 * Converts a DynamoDB row into a Exasol row
	 * 
	 * @param dynamodbRow
	 *            DynamoDB row
	 * @return Exasol row
	 * @throws AdapterException
	 *             if query abort was configured on conversion errors.
	 */
	public ExasolCellValue convertRow(final Map<String, AttributeValue> dynamodbRow) throws AdapterException {
		return this.columnMapping.convertRow(dynamodbRow);
	}

	AbstractColumnMappingDefinition getColumnMapping() {
		return this.columnMapping;
	}
}
