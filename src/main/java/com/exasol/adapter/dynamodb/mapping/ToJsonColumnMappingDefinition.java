package com.exasol.adapter.dynamodb.mapping;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.metadata.DataType;
import com.exasol.dynamodb.resultwalker.AbstractDynamodbResultWalker;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

/**
 * Maps a property of a DynamoDB table and all it's descendants to a JSON string
 */
public class ToJsonColumnMappingDefinition extends AbstractColumnMappingDefinition {
	private static final long serialVersionUID = 7687302490848045236L;

	/**
	 * Constructor.
	 * 
	 * @param destinationName
	 *            name of the column in Exasol table.
	 * @param resultWalker
	 *            source walker defining the path in DynamoDB documents.
	 */
	public ToJsonColumnMappingDefinition(final String destinationName, final AbstractDynamodbResultWalker resultWalker,
			final LookupFailBehaviour lookupFailBehaviour) {
		super(destinationName, resultWalker, lookupFailBehaviour);
	}

	@Override
	public DataType getDestinationDataType() {
		return DataType.createVarChar(10000, DataType.ExaCharset.UTF8);
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
	protected ValueExpression convertValue(final AttributeValue dynamodbProperty) {
		// TODO convert to json
		return StringLiteral.of("dummy value");
	}
}
