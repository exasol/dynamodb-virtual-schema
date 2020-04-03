package com.exasol.adapter.dynamodb.mapping.tostringmapping;

import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.ColumnMappingDefinitionVisitor;
import com.exasol.adapter.metadata.DataType;
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
	 * Creates an instance of {@link ToStringColumnMappingDefinition}.
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
	public void accept(final ColumnMappingDefinitionVisitor visitor) {
		visitor.visit(this);
	}

	/**
	 * Specifies the behaviour of {@link ToStringValueMapper} if the string from
	 * DynamoDB is longer than {@link #destinationStringSize}.
	 */
	public enum OverflowBehaviour {
		/**
		 * truncate the string to the configured length.
		 */
		TRUNCATE,

		/**
		 * throw an {@link ToStringValueMapper.OverflowException}.
		 */
		EXCEPTION
	}

}
