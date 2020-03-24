package com.exasol.adapter.dynamodb.mapping;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.StringSerializer;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.cellvalue.ExasolCellValue;

/**
 * Definition of a column mapping from DynamoDB table to Exasol Virtual Schema.
 * Each instance of this class represents one column in the Exasol table.
 * Objects of this class get serialized into the column adapter notes. They are
 * created using a {@link com.exasol.adapter.dynamodb.MappingProvider}. The
 * serialization can't be replaced by retrieving the mapping at query execution
 * time form the {@link com.exasol.adapter.dynamodb.MappingProvider} as the
 * definition (from bucketfs) could have changed but not been refreshed. Just
 * reloading the schema would lead to inconsistencies between the schema known
 * to the database and the one used by this adapter.
 */
public abstract class ColumnMappingDefinition implements Serializable {
	private static final long serialVersionUID = 48342992735371252L;
	private final String destinationName;

	public ColumnMappingDefinition(final String destinationName) {
		this.destinationName = destinationName;
	}


	/**
	 * Get the name of the column in the Exasol table.
	 * 
	 * @return name of the column
	 */
	public String getDestinationName() {
		return this.destinationName;
	}


	abstract DataType getDestinationDataType();
	abstract String getDestinationDefaultValue();
	abstract boolean isDestinationNullable();

	/**
	 * Extracts this columns value from DynamoDB's result row.
	 * 
	 * @param dynamodbRow
	 * @return {@link ExasolCellValue}
	 * @throws AdapterException
	 */
	public abstract ExasolCellValue convertRow(final Map<String, AttributeValue> dynamodbRow) throws AdapterException;
}
