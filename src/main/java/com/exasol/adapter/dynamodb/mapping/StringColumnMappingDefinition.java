package com.exasol.adapter.dynamodb.mapping;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.metadata.DataType;
import com.exasol.cellvalue.ExasolCellValue;
import com.exasol.cellvalue.StringExasolCellValue;
import com.exasol.dynamodb.resultwalker.DynamodbResultWalker;

/**
 * Extracts a string from a DynamoDB table
 */
public class StringColumnMappingDefinition extends ColumnMappingDefinition {
	private static final long serialVersionUID = -6772281079326146978L;
	private final int destinationStringSize;
	private final DynamodbResultWalker resultWalker;
	private final OverflowBehaviour overflowBehaviour;
	public StringColumnMappingDefinition(final String destinationName, final int destinationStringSize,
			final DynamodbResultWalker resultWalker, final OverflowBehaviour overflowBehaviour) {
		super(destinationName);
		this.destinationStringSize = destinationStringSize;
		this.resultWalker = resultWalker;
		this.overflowBehaviour = overflowBehaviour;
	}

	@Override
	DataType getDestinationDataType() {
		return DataType.createVarChar(this.destinationStringSize, DataType.ExaCharset.UTF8);// TODO @sebastian is ASCII
																							// intended?
	}

	@Override
	String getDestinationDefaultValue() {
		return null;
	}

	@Override
	boolean isDestinationNullable() {
		return true;
	}

	@Override
	public ExasolCellValue convertRow(final Map<String, AttributeValue> dynamodbRow) throws AdapterException {
		final String sourceString = walkToString(dynamodbRow);
		return new StringExasolCellValue(handleOverflow(sourceString));
	}

	private String walkToString(final Map<String, AttributeValue> dynamodbRow)
			throws DynamodbResultWalker.DynamodbResultWalkerException {
		final AttributeValue attributeValue = this.resultWalker.walk(dynamodbRow);
		if (attributeValue == null) {
			return null;
		}
		if (attributeValue.getS() != null) {
			return attributeValue.getS();
		} else if (attributeValue.getN() != null) {
			return attributeValue.getN();
		} else if (attributeValue.getBOOL() != null) {
			return Boolean.TRUE.equals(attributeValue.getBOOL()) ? "true" : "false";
		}
		return null;
	}

	private String handleOverflow(final String sourceString) throws OverflowException {
		if (sourceString == null) {
			return null;
		}
		if (sourceString.length() > this.destinationStringSize) {
			if (this.overflowBehaviour == OverflowBehaviour.TRUNCATE) {
				return sourceString.substring(0, this.destinationStringSize);
			} else {
				throw new OverflowException("String overflow");
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
	public static class OverflowException extends AdapterException {
		public OverflowException(final String message) {
			super(message);
		}
	}
}
