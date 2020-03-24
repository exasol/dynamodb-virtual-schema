package com.exasol.adapter.dynamodb.queryresult;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.mapping.ColumnMappingDefinition;
import com.exasol.cellvalue.ExasolCellValue;

/**
 * This class represents one column in a query result. It is used for building
 * the actual result using calls to {@link #convertRow(Map)}.
 *
 * Right now this class seems unnecessary as
 * {@link ColumnMappingDefinition#convertRow(Map)} could be used directly. It
 * will be needed in the future for representing constant values from the
 * {@code SELECT}.//TODO
 */
public class QueryResultColumn {
	private final ColumnMappingDefinition columnMapping;

	public QueryResultColumn(final ColumnMappingDefinition columnMapping) {
		this.columnMapping = columnMapping;
	}

	public ExasolCellValue convertRow(final Map<String, AttributeValue> dynamodbRow) throws AdapterException {
		return this.columnMapping.convertRow(dynamodbRow);
	}

	ColumnMappingDefinition getColumnMapping() {
		return this.columnMapping;
	}
}
