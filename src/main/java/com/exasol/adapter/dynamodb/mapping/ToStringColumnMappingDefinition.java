package com.exasol.adapter.dynamodb.mapping;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.metadata.DataType;
import com.exasol.dynamodb.attributevalue.AttributeValueVisitor;
import com.exasol.dynamodb.attributevalue.AttributeValueWrapper;
import com.exasol.dynamodb.resultwalker.AbstractDynamodbResultWalker;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * Extracts a string from a DynamoDB table and maps to a Exasol VarChar column
 */
public class ToStringColumnMappingDefinition extends AbstractColumnMappingDefinition {
	private static final long serialVersionUID = -6772281079326146978L;
	private final int destinationStringSize;
	private final OverflowBehaviour overflowBehaviour;

	/**
	 * Constructor.
	 * 
	 * @param destinationName
	 *            Name of the Exasol column
	 * @param destinationStringSize
	 *            Length of the Exasol VARCHAR
	 * @param resultWalker
	 *            {@link AbstractDynamodbResultWalker} representing the path to the
	 *            source * property
	 * @param lookupFailBehaviour
	 *            {@link LookupFailBehaviour} if the defined path does not exist
	 * @param overflowBehaviour
	 *            Behaviour if extracted string exceeds
	 *            {@link #destinationStringSize}
	 */
	public ToStringColumnMappingDefinition(final String destinationName, final int destinationStringSize,
			final AbstractDynamodbResultWalker resultWalker, final LookupFailBehaviour lookupFailBehaviour,
			final OverflowBehaviour overflowBehaviour) {
		super(destinationName, resultWalker, lookupFailBehaviour);
		this.destinationStringSize = destinationStringSize;
		this.overflowBehaviour = overflowBehaviour;
	}

	/**
	 * Get the maximum Exasol VARCHAR size.
	 * 
	 * @return maximum size of Exasol VARCHAR
	 */
	public int getDestinationStringSize() {
		return this.destinationStringSize;
	}

	/**
	 * Get the behaviour if the {@link #destinationStringSize} is exceeded.
	 * 
	 * @return {@link OverflowBehaviour}
	 */
	public OverflowBehaviour getOverflowBehaviour() {
		return this.overflowBehaviour;
	}

	@Override
	public DataType getDestinationDataType() {
		return DataType.createVarChar(this.destinationStringSize, DataType.ExaCharset.UTF8);
	}

	@Override
	public ValueExpression getDestinationDefaultValue() {
		return StringLiteral.of("");
	}

	@Override
	public boolean isDestinationNullable() {
		return true;
	}

	@Override
	protected ValueExpression convertValue(final AttributeValue dynamodbProperty) throws ColumnMappingException {
		final ToStringVisitor toStringVisitor = new ToStringVisitor();
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(dynamodbProperty);
		try {
			attributeValueWrapper.accept(toStringVisitor);
		} catch (final AttributeValueVisitor.UnsupportedDynamodbTypeException exception) {
			throw new LookupColumnMappingException(String.format("The DynamoDB type %s cant't be converted to string.",
					exception.getDynamodbTypeName()), this);
		}
		final String stringValue = toStringVisitor.getResult();
		if (stringValue == null) {
			return getDestinationDefaultValue();
		}
		return StringLiteral.of(this.handleOverflow(stringValue));
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
	 * DynamoDB is longer than {@link #destinationStringSize}.
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
	 * Visitor for {@link AttributeValue} that converts its value to string. If this
	 * is not possible an {@link UnsupportedOperationException} is thrown.
	 */
	private static class ToStringVisitor implements AttributeValueVisitor {
		private String stringValue;

		@Override
		public void visitString(final String value) {
			this.stringValue = value;
		}

		@Override
		public void visitNumber(final String value) {
			this.stringValue = value;
		}

		@Override
		public void visitNull() {
			this.stringValue = null;
		}

		@Override
		public void visitBoolean(final boolean value) {
			this.stringValue = Boolean.TRUE.equals(value) ? "true" : "false";
		}

		public String getResult() {
			return this.stringValue;
		}
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
