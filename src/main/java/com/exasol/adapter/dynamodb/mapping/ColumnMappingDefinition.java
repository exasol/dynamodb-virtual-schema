package com.exasol.adapter.dynamodb.mapping;

import java.io.Serializable;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.metadata.DataType;
import com.exasol.cellvalue.ExasolCellValue;
import com.exasol.dynamodb.resultwalker.DynamodbResultWalker;

/**
 * Definition of a column mapping from DynamoDB table to Exasol Virtual Schema.
 * Each instance of this class represents one column in the Exasol table.
 * Objects of this class get serialized into the column adapter notes. They are
 * created using a {@link MappingFactory}. The serialization can't be replaced
 * by retrieving the mapping at query execution time form the
 * {@link MappingFactory} as the definition (from bucketfs) could have changed
 * but not been refreshed. Just reloading the schema would lead to
 * inconsistencies between the schema known to the database and the one used by
 * this adapter.
 */
public abstract class ColumnMappingDefinition implements Serializable {
	private static final long serialVersionUID = 48342992735371252L;
	private final String destinationName;
	private final DynamodbResultWalker resultWalker;
	private final LookupFailBehaviour lookupFailBehaviour;

	public ColumnMappingDefinition(final String destinationName, final DynamodbResultWalker resultWalker,
			final LookupFailBehaviour lookupFailBehaviour) {
		this.destinationName = destinationName;
		this.resultWalker = resultWalker;
		this.lookupFailBehaviour = lookupFailBehaviour;
	}

	/**
	 * Get the name of the column in the Exasol table.
	 * 
	 * @return name of the column
	 */
	public String getDestinationName() {
		return this.destinationName;
	}

	public abstract DataType getDestinationDataType();
	public abstract ExasolCellValue getDestinationDefaultValue();
	public abstract boolean isDestinationNullable();

	public LookupFailBehaviour getLookupFailBehaviour() {
		return this.lookupFailBehaviour;
	}

	/**
	 * Extracts this columns value from DynamoDB's result row.
	 *
	 * @param dynamodbRow
	 * @return {@link ExasolCellValue}
	 * @throws AdapterException
	 */
	public ExasolCellValue convertRow(final Map<String, AttributeValue> dynamodbRow)
			throws DynamodbResultWalker.DynamodbResultWalkerException, ColumnMappingException {
		try {
			final AttributeValue dynamodbProperty = this.resultWalker.walk(dynamodbRow);
			return convertValue(dynamodbProperty);
		} catch (final DynamodbResultWalker.DynamodbResultWalkerException e) {
			if (this.lookupFailBehaviour == LookupFailBehaviour.DEFAULT_VALUE) {
				return this.getDestinationDefaultValue();
			}
			throw e;
		}
	}

	protected abstract ExasolCellValue convertValue(AttributeValue dynamodbProperty) throws ColumnMappingException;

	/**
	 * Behaviour if the requested property is not set in a given DynamoDB row.
	 */
	public enum LookupFailBehaviour {
		/**
		 * Break the whole query.
		 */
		EXCEPTION,
		/**
		 * Set column value to null.
		 */
		DEFAULT_VALUE
	}

	public static class ColumnMappingException extends AdapterException {

		public ColumnMappingException(final String message) {
			super(message);
		}
	}
}
