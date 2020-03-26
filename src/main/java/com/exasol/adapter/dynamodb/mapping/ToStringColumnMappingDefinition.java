package com.exasol.adapter.dynamodb.mapping;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.metadata.DataType;
import com.exasol.cellvalue.ExasolCellValue;
import com.exasol.cellvalue.StringExasolCellValue;
import com.exasol.dynamodb.resultwalker.DynamodbResultWalker;

/**
 * Extracts a string from a DynamoDB table
 */
public class ToStringColumnMappingDefinition extends ColumnMappingDefinition {
	private static final long serialVersionUID = -6772281079326146978L;
	private final int destinationStringSize;
	private final OverflowBehaviour overflowBehaviour;
	public ToStringColumnMappingDefinition(final String destinationName, final int destinationStringSize,
			final DynamodbResultWalker resultWalker, final LookupFailBehaviour lookupFailBehaviour,
			final OverflowBehaviour overflowBehaviour) {
		super(destinationName, resultWalker, lookupFailBehaviour);
		this.destinationStringSize = destinationStringSize;
		this.overflowBehaviour = overflowBehaviour;
	}

	public int getDestinationStringSize() {
		return this.destinationStringSize;
	}

	public OverflowBehaviour getOverflowBehaviour() {
		return this.overflowBehaviour;
	}

	@Override
	public DataType getDestinationDataType() {
		return DataType.createVarChar(this.destinationStringSize, DataType.ExaCharset.UTF8);// TODO @sebastian is ASCII
																							// intended?
	}

	@Override
	public ExasolCellValue getDestinationDefaultValue() {
		return new StringExasolCellValue("");
	}

	@Override
	public boolean isDestinationNullable() {
		return true;
	}

	@Override
	protected ExasolCellValue convertValue(final AttributeValue dynamodbProperty) throws ColumnMappingException {
		final String sourceString = walkToString(dynamodbProperty);
		return new StringExasolCellValue(handleOverflow(sourceString));
	}

	private String walkToString(final AttributeValue attributeValue) throws ColumnMappingException {
		if (attributeValue.getS() != null) {
			return attributeValue.getS();
		} else if (attributeValue.getN() != null) {
			return attributeValue.getN();
		} else if (attributeValue.getBOOL() != null) {
			return Boolean.TRUE.equals(attributeValue.getBOOL()) ? "true" : "false";
		}
		throw new UnsupportedDynamodbTypeException("this attribute value type can't be converted to string", this);
	}

	private String handleOverflow(final String sourceString) throws OverflowException {
		if (sourceString.length() > this.destinationStringSize) {
			if (this.overflowBehaviour == OverflowBehaviour.TRUNCATE) {
				return sourceString.substring(0, this.destinationStringSize);
			} else {
				throw new OverflowException("String overflow", this);
			}
		}
		return sourceString;
	}

	/**
	 * Specifies the behaviour of {@link #convertRow(Map)} if the string from
	 * DynamoDB is longer than the configured size.
	 */
	public enum OverflowBehaviour {
		/**
		 * truncate the string to the configured length.
		 */
		TRUNCATE,

		/**
		 * throw an {@link OverflowException}.
		 */
		EXCEPTION
	}

	/**
	 * Exception thrown if the size of the string from DynamoDB is longer than the
	 * configured size.
	 */
	@SuppressWarnings("serial")
	public static class OverflowException extends ColumnMappingException {
		public OverflowException(final String message, final ToStringColumnMappingDefinition column) {
			super(message, column);
		}
	}
}
